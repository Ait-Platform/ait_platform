"""Read-only deployed HOME source parity evidence; never starts Flask or renders PDFs.

Run on the deployed tree with an explicitly supplied PostgreSQL credential environment
variable. Credentials are never written to the evidence. Output is exclusive-create;
existing attestations/manifests cannot be overwritten. This checker does not attest.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def load_snapshot_module(root):
    path = root / "app/program_sace_home/manual_sources.py"
    spec = importlib.util.spec_from_file_location("home_parity_snapshot", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def differences(expected, actual, path="source"):
    """Exact JSON differences, including missing rows, source text and asset hashes."""
    if type(expected) is not type(actual):
        return [{"path": path, "expected": expected, "actual": actual}]
    changes = []
    if isinstance(expected, dict):
        for key in sorted(set(expected) | set(actual)):
            location = path + "." + str(key)
            if key not in expected:
                changes.append({"path": location, "missing": "expected", "actual": actual[key]})
            elif key not in actual:
                changes.append({"path": location, "missing": "actual", "expected": expected[key]})
            else:
                changes.extend(differences(expected[key], actual[key], location))
    elif isinstance(expected, list):
        for index in range(max(len(expected), len(actual))):
            location = path + "[" + str(index) + "]"
            if index >= len(expected):
                changes.append({"path": location, "missing": "expected", "actual": actual[index]})
            elif index >= len(actual):
                changes.append({"path": location, "missing": "actual", "expected": expected[index]})
            else:
                changes.extend(differences(expected[index], actual[index], location))
    elif expected != actual:
        changes.append({"path": path, "expected": expected, "actual": actual})
    return changes


def compare_manifest(manifest, snapshot, root, canonical):
    expected = manifest.get("source")
    digest = lambda value: hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()
    errors = []
    if not isinstance(expected, dict) or digest(expected) != manifest.get("source_sha256"):
        errors.append("Manifest source does not match its canonical SHA-256.")
    if digest(snapshot["source"]) != snapshot["source_sha256"]:
        errors.append("Actual snapshot canonical SHA-256 is inconsistent.")
    if manifest.get("builder_version") != snapshot.get("builder_version"):
        errors.append("Snapshot builder version differs.")
    if manifest.get("gaps") or snapshot.get("gaps"):
        errors.append("Source gaps are present.")
    inputs = manifest.get("current_provenance", {}).get("current_additional_inputs", {})
    if not inputs:
        errors.append("Additional manifest inputs are missing.")
    input_results = []
    for relative, expected_hash in sorted(inputs.items()):
        path = (root / relative).resolve()
        if (not path.is_relative_to(root) or path == root or "\\" in relative
                or any(p in {"", ".", ".."} for p in relative.split("/"))):
            input_results.append({"path": relative, "error": "Invalid input path.", "matches": False})
            continue
        try:
            actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            input_results.append({"path": relative, "expected": expected_hash,
                "actual": actual_hash, "matches": actual_hash == expected_hash})
        except OSError:
            input_results.append({"path": relative, "error": "Input unavailable.", "matches": False})
    changes = differences(expected, snapshot["source"])
    return {"kind": manifest.get("kind"), "expected_sha256": manifest.get("source_sha256"),
        "actual_sha256": snapshot["source_sha256"], "differences": changes,
        "gaps": snapshot["gaps"], "additional_inputs": input_results, "errors": errors,
        "matches": not errors and not changes
            and snapshot["source_sha256"] == manifest.get("source_sha256")
            and all(item["matches"] for item in input_results)}


def capture(engine, root, module):
    if engine.dialect.name != "postgresql":
        raise ValueError("Parity requires PostgreSQL.")
    with engine.connect() as connection:
        connection = connection.execution_options(
            isolation_level="REPEATABLE READ", postgresql_readonly=True)
        transaction = connection.begin()
        try:
            readonly = connection.execute(text("SHOW transaction_read_only")).scalar_one()
            isolation = connection.execute(text("SHOW transaction_isolation")).scalar_one()
            if readonly != "on" or isolation.lower() != "repeatable read":
                raise ValueError("Read-only repeatable-read enforcement failed.")
            identity = dict(connection.execute(text(
                "SELECT current_database() AS database, current_user AS role")).mappings().one())
            snapshot = module.source_snapshot(connection, root)
            return snapshot, {"transaction_read_only": readonly,
                "transaction_isolation": isolation, **identity}
        finally:
            transaction.rollback()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="Actual deployed application checkout.")
    parser.add_argument("--database-env", required=True, help="Environment variable holding the verified read-only DB URL.")
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    manifest_bytes = [path.read_bytes() for path in args.manifest]
    manifests = [json.loads(raw.decode("utf-8")) for raw in manifest_bytes]
    if (len(manifests) != 2 or {m.get("kind") for m in manifests}
            != {"participant_manual", "facilitator_manual"}):
        parser.error("Supply exactly the two approved HOME manual manifests.")
    output = args.output.resolve()
    protected = [root / "app/static", root / "output", *(p.resolve() for p in args.manifest)]
    if output.suffix.lower() != ".json" or any(output == p or output.is_relative_to(p) for p in protected):
        parser.error("Use a new JSON evidence path outside served/artifact inputs.")
    if output.exists():
        parser.error("Evidence output already exists; select a new append-only evidence path.")
    raw_url = os.environ.get(args.database_env)
    if not raw_url:
        parser.error("The named database credential environment variable is absent.")
    url = make_url(raw_url)
    if url.get_backend_name() != "postgresql":
        parser.error("Parity requires PostgreSQL.")
    module = load_snapshot_module(root)
    # Enforce read-only at connection startup as well as transaction startup.
    engine = create_engine(url, connect_args={"options": "-c default_transaction_read_only=on"})
    try:
        snapshot, database = capture(engine, root, module)
    finally:
        engine.dispose()
    comparisons = [compare_manifest(m, snapshot, root, module.canonical) for m in manifests]
    evidence = {"schema": "home-production-parity-evidence-v1",
        "captured_at": datetime.now(timezone.utc).isoformat(), "deployed_root": str(root),
        "checker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "snapshot_implementation_sha256": hashlib.sha256(
            (root / "app/program_sace_home/manual_sources.py").read_bytes()).hexdigest(),
        "database": database, "snapshot": snapshot, "comparisons": comparisons,
        "manifest_inputs": [{"path": str(p.resolve()),
            "sha256": hashlib.sha256(raw).hexdigest()}
            for p, raw in zip(args.manifest, manifest_bytes)],
        "matches": all(c["matches"] for c in comparisons),
        "attestation": None}
    with output.open("x", encoding="utf-8") as stream:
        json.dump(evidence, stream, ensure_ascii=False, sort_keys=True, indent=2)
    print("Parity:", "MATCH" if evidence["matches"] else "MISMATCH")
    for comparison in comparisons:
        print(comparison["kind"], "differences:", len(comparison["differences"]),
            "gaps:", len(comparison["gaps"]), "matches:", comparison["matches"])
    return 0 if evidence["matches"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
