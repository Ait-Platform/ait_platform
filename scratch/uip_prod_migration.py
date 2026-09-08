"""Explicitly authorised, backup-first UIP production migration coordinator.

Credentials are read from existing local configuration, never printed/persisted.
No application factory, application deployment or business/test writes.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime, timezone
import psycopg2
from psycopg2 import sql
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations/versions/uip_p49_operations.py"
PG_BIN = Path("C:/Program Files/PostgreSQL/18/bin")
SOURCES = ("check_render_db.py", "migrate_prod.py", "check_render_tables.py", "scratch/update_render_db.py")
PATTERN = re.compile(r"postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s\"'`<>]+", re.I)
# Explicit user scope: traffic logs are outside all UIP preservation comparisons.
PRESERVATION_EXCLUDED_TABLES = frozenset({"site_hit", "visit_log"})


def candidates():
    found = {}
    for name in SOURCES:
        path = ROOT / name
        if not path.is_file():
            continue
        for value in PATTERN.findall(path.read_text(encoding="utf-8-sig", errors="replace")):
            try:
                url = make_url(value.rstrip(";,)").replace("postgres://", "postgresql://", 1))
            except Exception:
                continue
            if url.host and url.host.endswith(".render.com") and url.password and not set(url.query) - {"sslmode"}:
                found.setdefault((url.host, url.port or 5432, url.database), (url, name))
    return list(found.values())


def production_url():
    choices = candidates()
    if len(choices) != 1:
        for url, source in choices:
            print("Candidate:", url.host, url.database, "source:", source)
        raise RuntimeError("Expected exactly one identifiable Render production database")
    return choices[0][0]


def connect(url):
    connection = psycopg2.connect(host=url.host, port=url.port or 5432, dbname=url.database,
        user=url.username, password=url.password, sslmode="require", connect_timeout=15,
        application_name="uip_p49_authorized_migration_verification")
    connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
    return connection


def revision(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT version_num FROM public.alembic_version ORDER BY version_num")
        return [row[0] for row in cursor]


def migration_module():
    spec = importlib.util.spec_from_file_location("production_uip_p49", MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if module.revision != "uip_p49_operations" or module.down_revision != "uip_p3_work_orders":
        raise RuntimeError("Unexpected migration revision or parent")
    return module


def prerequisites(connection):
    module = migration_module()
    if revision(connection) != ["uip_p3_work_orders"]:
        raise RuntimeError("STOP: production revision is not exactly uip_p3_work_orders")
    additions = [list(pair) for pair in re.findall(r'ALTER TABLE "(\w+)" ADD COLUMN (\w+)', "\n".join(module.DDL))]
    new_tables = re.findall(r'CREATE TABLE (\w+)', "\n".join(module.DDL))
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), current_schema(), current_setting('server_version'), pg_is_in_recovery()")
        database, schema, server_version, replica = cursor.fetchone()
        if schema != "public" or replica:
            raise RuntimeError("Unexpected production schema or read replica")
        cursor.execute("SELECT count(*) FROM pg_event_trigger WHERE evtenabled <> 'D'")
        if cursor.fetchone()[0]:
            raise RuntimeError("STOP: unexpected database event triggers require review")
        cursor.execute("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname LIKE 'uip_p49_%'")
        if cursor.fetchone()[0]:
            raise RuntimeError("STOP: Phase 4–9 functions already exist")
        cursor.execute("SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('public.uip_resolution'::regclass, 'public.uip_municipal_referral'::regclass)")
        if cursor.fetchone()[0]:
            raise RuntimeError("STOP: unexpected triggers on backfilled UIP tables")
        cursor.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public'")
        existing_columns = set(cursor.fetchall())
        cursor.execute("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'")
        existing_relations = {row[0] for row in cursor}
        for table, column in additions:
            if (table, column) in existing_columns:
                raise RuntimeError("STOP: Phase 4–9 column already exists: " + table + "." + column)
        for table in new_tables:
            if table in existing_relations:
                raise RuntimeError("STOP: Phase 4–9 table already exists: " + table)
        for table in ("core_organization", "core_interaction", "core_task", "uip_provider", "uip_work_order",
                "uip_work_order_action", "uip_provider_capability", "uip_provider_user", "uip_member_profile",
                "uip_property", "uip_property_member", "uip_member_representative", "uip_audit_event",
                "uip_document", "uip_committee_meeting", "uip_resolution", "uip_municipal_referral"):
            if table not in existing_relations:
                raise RuntimeError("STOP: prerequisite table missing: " + table)
        cursor.execute("SELECT count(*) FROM public.uip_municipal_referral r LEFT JOIN public.core_interaction i ON i.id=r.interaction_id WHERE i.id IS NULL OR i.organization_id IS NULL")
        if cursor.fetchone()[0]:
            raise RuntimeError("STOP: municipal referral cannot be safely organisation-scoped")
        cursor.execute("SELECT count(*) FROM public.uip_resolution r LEFT JOIN public.uip_committee_meeting m ON m.id=r.meeting_id WHERE m.id IS NULL OR m.organization_id IS NULL")
        if cursor.fetchone()[0]:
            raise RuntimeError("STOP: resolution cannot be safely organisation-scoped")
        for table in ("core_interaction", "uip_work_order", "uip_member_profile"):
            cursor.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND contype IN ('p','u')", ("public." + table,))
            if not any("(id, organization_id)" in row[0] for row in cursor):
                raise RuntimeError("STOP: missing tenant identity prerequisite on " + table)
    return dict(database=database, schema=schema, server_version=server_version,
        revision=["uip_p3_work_orders"], migration=module.revision, additions=additions, new_tables=new_tables,
        migration_sha256=hashlib.sha256(MIGRATION.read_bytes()).hexdigest())


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pg_environment(url):
    environment = dict(os.environ)
    for key in list(environment):
        if key.startswith("PG"):
            environment.pop(key)
    environment.update(PGHOST=url.host, PGPORT=str(url.port or 5432), PGDATABASE=url.database,
        PGUSER=url.username, PGPASSWORD=url.password, PGSSLMODE="require", PGCONNECT_TIMEOUT="15")
    return environment


def catalog(connection):
    tables = {}
    with connection.cursor() as cursor:
        cursor.execute("""SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind IN ('r','p') AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
            ORDER BY 1,2""")
        for schema, name in cursor:
            if name in PRESERVATION_EXCLUDED_TABLES:
                continue
            tables[schema + "." + name] = dict(schema=schema, name=name, columns=[], constraints=[], indexes=[], triggers=[])
        cursor.execute("""SELECT table_schema, table_name, column_name, data_type, udt_schema, udt_name,
            is_nullable, column_default, ordinal_position, collation_name, is_identity, identity_generation,
            is_generated, generation_expression FROM information_schema.columns
            WHERE table_schema NOT LIKE 'pg_%' AND table_schema <> 'information_schema' ORDER BY 1,2,9""")
        for row in cursor:
            key = row[0] + "." + row[1]
            if key in tables:
                tables[key]["columns"].append(list(row[2:]))
        for kind, query in (
            ("constraints", "SELECT n.nspname,c.relname,k.conname,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY 1,2,3"),
            ("indexes", "SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes ORDER BY 1,2,3"),
            ("triggers", "SELECT n.nspname,c.relname,t.tgname,pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal ORDER BY 1,2,3")):
            cursor.execute(query)
            for schema, name, object_name, definition in cursor:
                key = schema + "." + name
                if key in tables:
                    tables[key][kind].append([object_name, definition])
    return tables


def fingerprints(connection, tables):
    result = {}
    modulus = 1 << 256
    for index, (key, table) in enumerate(tables.items()):
        columns = sql.SQL(",").join(sql.Identifier(column[0]) for column in table["columns"])
        query = sql.SQL("SELECT encode(sha256(convert_to(row_to_json(r)::text,'UTF8')),'hex') FROM (SELECT {} FROM {}.{}) r").format(
            columns, sql.Identifier(table["schema"]), sql.Identifier(table["name"]))
        count = total = xor = 0
        with connection.cursor(name="uip_preserve_" + str(index)) as cursor:
            cursor.itersize = 5000
            cursor.execute(query)
            for (value,) in cursor:
                number = int(value, 16)
                count += 1
                total = (total + number) % modulus
                xor ^= number
        result[key] = dict(rows=count, sum_sha256=f"{total:064x}", xor_sha256=f"{xor:064x}")
        if (index + 1) % 25 == 0:
            print("Read-only preservation fingerprints:", index + 1, "/", len(tables), flush=True)
    return result


def backup(url):
    with connect(url) as connection:
        pre = prerequisites(connection)
        version = subprocess.run([str(PG_BIN / "pg_dump.exe"), "--version"], capture_output=True, text=True, check=True).stdout.strip()
        if " 18." not in version or not pre["server_version"].startswith("18."):
            raise RuntimeError("STOP: backup client/server version prerequisite differs")
        base = ROOT / "var/backups/uip_production"
        base.mkdir(parents=True, exist_ok=True)
        (base / ".gitignore").write_text("*\n", encoding="utf-8")
        directory = base / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        directory.mkdir()
        path = directory / "before_uip_p49.dump"
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_export_snapshot()")
            snapshot = cursor.fetchone()[0]
        print("Taking full PostgreSQL custom-format backup with pg_dump 18...", flush=True)
        result = subprocess.run([str(PG_BIN / "pg_dump.exe"), "--format=custom", "--file=" + str(path),
            "--snapshot=" + snapshot, "--lock-wait-timeout=15s"], env=pg_environment(url), capture_output=True)
        if result.returncode:
            raise RuntimeError("STOP: pg_dump failed; no migration performed")
        # Fully read/decompress the archive to a discarded SQL stream; no database restore.
        checked = subprocess.run([str(PG_BIN / "pg_restore.exe"), "--file=-", str(path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if checked.returncode or not path.stat().st_size:
            raise RuntimeError("STOP: backup archive validation failed")
        manifest = dict(preflight=pre, host=url.host, backup=str(path), backup_bytes=path.stat().st_size,
            backup_sha256=checksum(path), archive_read_verified=True, created_utc=datetime.now(timezone.utc).isoformat())
        manifest_path = directory / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print("BACKUP SUCCEEDED", flush=True)
        print(json.dumps(manifest, indent=2), flush=True)
        print("MANIFEST:", manifest_path, flush=True)


def migrate_and_verify(url, manifest_path):
    manifest_path = Path(manifest_path).resolve()
    expected_base = (ROOT / "var/backups/uip_production").resolve()
    if not manifest_path.is_relative_to(expected_base):
        raise RuntimeError("STOP: unexpected backup manifest location")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    backup_path = Path(manifest["backup"]).resolve()
    if not backup_path.is_relative_to(manifest_path.parent) or not manifest["archive_read_verified"] or checksum(backup_path) != manifest["backup_sha256"]:
        raise RuntimeError("STOP: backup verification does not match")
    with connect(url) as connection:
        pre = prerequisites(connection)
        if pre != manifest["preflight"] or url.host != manifest["host"]:
            raise RuntimeError("STOP: prerequisites changed since backup")
        tables = catalog(connection)
        before = fingerprints(connection, tables)
    manifest["before_catalog"], manifest["before_data"] = tables, before
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    # Recheck the exact revision immediately before the only migration command.
    with connect(url) as connection:
        prerequisites(connection)
    environment = pg_environment(url)
    environment.update(DATABASE_URL=url.render_as_string(hide_password=False),
        SQLALCHEMY_DATABASE_URI=url.render_as_string(hide_password=False),
        PGOPTIONS="-c lock_timeout=15s -c statement_timeout=120s")
    command = [sys.executable, "-B", "-m", "alembic", "-c", "alembic.ini", "upgrade", "uip_p49_operations"]
    print("Running only: python -B -m alembic -c alembic.ini upgrade uip_p49_operations", flush=True)
    result = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True)
    safe_log = (result.stdout + result.stderr).replace(url.render_as_string(hide_password=False), "[REDACTED]").replace(url.password, "[REDACTED]")
    (manifest_path.parent / "migration.log").write_text(safe_log, encoding="utf-8")
    manifest["migration_returncode"] = result.returncode
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if result.returncode:
        raise RuntimeError("STOP: migration command failed; inspect retained migration.log before further action")
    print(safe_log, flush=True)
    with connect(url) as connection:
        final_revision = revision(connection)
        after_catalog = catalog(connection)
        after = fingerprints(connection, tables)
        altered = {"public." + table for table, column in pre["additions"]}
        new_expected = {"public." + name for name in pre["new_tables"]}
        problems = []
        if final_revision != ["uip_p49_operations"]:
            problems.append("Unexpected final Alembic revision")
        if set(after_catalog) - set(tables) != new_expected or set(tables) - set(after_catalog):
            problems.append("Unexpected table-set change")
        for statement in migration_module().DDL:
            match = re.match(r"CREATE TABLE (\w+)", statement.strip())
            if match:
                name = "public." + match.group(1)
                expected_columns = set(re.findall(r"^\s*([a-z_][a-z_0-9]*)\s+(?:SERIAL|INTEGER|VARCHAR|TIMESTAMP|BOOLEAN|JSON|DATE|TEXT)\b", statement, re.M | re.I))
                actual_columns = {column[0] for column in after_catalog.get(name, {}).get("columns", [])}
                if actual_columns != expected_columns:
                    problems.append("New-table column mismatch: " + name)
        changed_data = [name for name in tables if name != "public.alembic_version" and before[name] != after[name]]
        changed_unrelated_schema = [name for name in tables if name not in altered and tables[name] != after_catalog.get(name)]
        if changed_data:
            problems.append("Existing row fingerprints differ: " + ", ".join(changed_data))
        if changed_unrelated_schema:
            problems.append("Unrelated schema changed: " + ", ".join(changed_unrelated_schema))
        for name in altered:
            original = {column[0]: column for column in tables[name]["columns"]}
            current = {column[0]: column for column in after_catalog[name]["columns"]}
            expected_added = {column for table, column in pre["additions"] if "public." + table == name}
            if set(current) - set(original) != expected_added:
                problems.append("Unexpected columns in " + name)
            for column_name, definition in original.items():
                expected = list(definition)
                if name == "public.uip_resolution" and column_name == "meeting_id":
                    expected[4] = "YES"  # is_nullable
                if current.get(column_name) != expected:
                    problems.append("Unexpected original-column change: " + name + "." + column_name)
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM public.uip_municipal_referral r JOIN public.core_interaction i ON i.id=r.interaction_id WHERE r.organization_id IS DISTINCT FROM i.organization_id")
            if cursor.fetchone()[0]:
                problems.append("Referral organisation backfill mismatch")
            cursor.execute("SELECT count(*) FROM public.uip_resolution r JOIN public.uip_committee_meeting m ON m.id=r.meeting_id WHERE r.organization_id IS DISTINCT FROM m.organization_id")
            if cursor.fetchone()[0]:
                problems.append("Decision organisation backfill mismatch")
            for table in pre["new_tables"]:
                cursor.execute(sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(table)))
                if cursor.fetchone()[0]:
                    problems.append("Unexpected new business rows in " + table)
        upgrades = re.findall(r"Running upgrade ([^\r\n]+)", safe_log)
        if len(upgrades) != 1 or not upgrades[0].startswith("uip_p3_work_orders -> uip_p49_operations"):
            problems.append("Migration log does not show exactly the authorised upgrade")
    report = dict(backup=manifest["backup"], backup_sha256=manifest["backup_sha256"],
        pre_revision=pre["revision"], migration_result="succeeded", final_revision=final_revision,
        expected_new_tables=len(new_expected), expected_added_columns=len(pre["additions"]),
        existing_tables_checked=len(tables), unchanged_existing_data=not changed_data,
        preservation_excluded_tables=sorted(PRESERVATION_EXCLUDED_TABLES),
        unrelated_schema_unchanged=not changed_unrelated_schema, verification_problems=problems,
        safe_to_redeploy=not problems, no_application_deployment=True)
    manifest["verification"] = report
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    if problems:
        raise RuntimeError("STOP: verification requires review; no further mutation performed")


def main():
    if sys.argv[1] == "identify":
        for url, source in candidates():
            print("Configured Render database:", url.host, "database:", url.database, "source:", source)
        return
    url = production_url()
    if sys.argv[1] == "backup":
        backup(url)
        return
    if sys.argv[1] == "migrate":
        migrate_and_verify(url, sys.argv[2])
        return
    with connect(url) as connection:
        result = prerequisites(connection)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Never emit raw driver connection strings or credentials.
        print("STOP:", str(error) if isinstance(error, RuntimeError) else type(error).__name__)
        sys.exit(1)
