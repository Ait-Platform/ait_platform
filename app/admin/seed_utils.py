# app/admin/seed_utils.py
from __future__ import annotations
import csv
import io
import os
from pathlib import Path
from typing import Iterable, List, Dict, Tuple
from flask import current_app, Response
from sqlalchemy.exc import SQLAlchemyError
from io import StringIO
from sqlalchemy import text, inspect
from app.extensions import db  

# --- Canonical registry of seeds -------------------------------------------
from pathlib import Path


# --- Seed repo helpers -------------------------------------------------------
from pathlib import Path

# Normalize seed keys used across UI/routes/files
def canon_seed(seed: str) -> str:
    s = (seed or "").strip().lower().replace(" ", "-").replace("_", "-")
    aliases = {
        "phase-item": "phase-items", "phase_items": "phase-items",
        "progress-item": "progress-items", "progress_items": "progress-items",
        "overall-item": "overall-items", "overall_items": "overall-items",
    }
    return aliases.get(s, s)

# Where we look for CSVs in your project (first match wins)
#  - <repo_root>/seeds/loss/*.csv          (preferred)
#  - <repo_root>/app/school_loss/seed/*.csv  (back-compat)
_REPO_SEED_DIRS = [
    Path(__file__).resolve().parents[2] / "seeds" / "loss",
    Path(__file__).resolve().parents[1] / "school_loss" / "seed",
]

# Default filenames per seed key (fallback is f"{seed}.csv")
_DEFAULT_SEED_FILES = {
    "questions":       "questions.csv",
    "phase-items":     "phase_items.csv",
    "progress-items":  "progress_items.csv",
    "overall-items":   "overall_items.csv",
    "instruction":     "instruction.csv",
    "explain":         "explain.csv",
    "pause":           "pause.csv",   # only if you use pause cards
}

def import_from_repo(seed_key: str) -> int:
    """
    Find a CSV for `seed_key` in the repo and import it using import_csv_stream.
    Returns number of rows imported. Raises FileNotFoundError if not found.
    """
    # import_csv_stream must already exist in this module (as you have)
    # or import from where you defined it:
    # from app.admin.seed_utils import import_csv_stream  # if needed

    seed_key = canon_seed(seed_key)
    filename = _DEFAULT_SEED_FILES.get(seed_key, f"{seed_key}.csv")

    csv_path = None
    for base in _REPO_SEED_DIRS:
        candidate = base / filename
        if candidate.exists():
            csv_path = candidate
            break

    if not csv_path:
        tried = [str(p / filename) for p in _REPO_SEED_DIRS]
        raise FileNotFoundError(
            f"No seed CSV found for '{seed_key}'. Tried: {tried}"
        )

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return import_csv_stream(seed_key, f)
'''
# Where to look for seed CSVs in the repo (first match wins).
#  - ./seeds/loss/*.csv              ← new preferred location
#  - ./app/school_loss/seed/*.csv    ← backward-compat (old location)
_REPO_SEED_DIRS = [
    Path(__file__).resolve().parents[2] / "seeds" / "loss",
    Path(__file__).resolve().parents[1] / "school_loss" / "seed",
]

# Default CSV filenames per seed key (fallback: "<seed>.csv")
_DEFAULT_SEED_FILES = {
    "questions":        "questions.csv",
    "phase-items":      "phase_items.csv",
    "progress-items":   "progress_items.csv",
    "overall-items":    "overall_items.csv",
    "instruction":      "instruction.csv",
    "explain":          "explain.csv",
    "pause":            "pause.csv",   # if you add pause cards
}
'''
SEED_TABLES = {
    # existing seeds...
    "questions": {
        "table": "lca_question",
        "pk": "id",
        "cols": ["id", "number", "text", "title", "caption", "buttons"],
        "title": "Questions",
        "note":  "Looks for seeds/questions.csv",
    },
    "phase_items": {
        "table": "lca_phase_item",
        "pk": "id",
        "cols": ["id", "phase_id", "ordinal", "body", "active"],
        "title": "Phase Items",
        "note":  "Looks for seeds/phase_items.csv",
    },
    "progress_items": {
        "table": "lca_progress_item",
        "pk": "id",
        "cols": ["id", "phase_id", "band", "tone", "body", "ordinal", "active"],
        "title": "Progress Items",
        "note":  "Looks for seeds/progress_items.csv",
    },
    "overall_items": {
        "table": "lca_overall_item",   # ensure this table exists, or remove this block
        "pk": "id",
        "cols": ["id", "band", "tone", "body", "ordinal", "active"],
        "title": "Overall Items",
        "note":  "Looks for seeds/overall_items.csv",
    },
    "intro_cards": {
        "table": "lca_instruction",
        "pk": "id",
        "cols": ["id", "title", "caption", "content"],
        "title": "Introduction Cards",
        "note":  "Looks for seeds/intro_cards.csv",
    },
    "explain_cards": {
        "table": "lca_explain",
        "pk": "id",
        "cols": ["id", "title", "caption", "content"],
        "title": "Explain Cards",
        "note":  "Looks for seeds/explain_cards.csv",
    },
    "pause_cards": {                     # only if you created lca_pause
        "table": "lca_pause",
        "pk": "id",
        "cols": ["id", "title", "caption", "content"],
        "title": "Pause Cards",
        "note":  "Looks for seeds/pause_cards.csv",
    },
}

# --- Aliases so old tabs/URLs keep working ---------------------------------
SEED_ALIASES = {
    "instruction": "intro_cards",
    "instructions": "intro_cards",
    "intro": "intro_cards",
    "explain": "explain_cards",
    "pause": "pause_cards",
}
'''
def canon_seed(seed: str) -> str:
    s = (seed or "").strip().lower()
    return SEED_ALIASES.get(s, s)
'''

def db_has_table(name: str) -> bool:
    return inspect(db.engine).has_table(name)

def to_bool(v):
    if v is None: return None
    return str(v).strip().lower() in ("1","true","t","yes","y")

def to_int(v):
    if v in (None, ""): return None
    try: return int(v)
    except: return None

def normalize_row(seed_key: str, row: dict) -> dict:
    meta = SEED_TABLES[seed_key]
    out = {}
    for col in meta["cols"]:
        val = row.get(col)
        if col in ("id","phase_id","ordinal","number"):
            val = to_int(val)
        elif col == "active":
            val = to_bool(val)
        elif isinstance(val, str):
            val = val.replace("\ufeff", "").strip()
        out[col] = val
    return out

def import_csv_stream(seed_key: str, file_like) -> int:
    if seed_key not in SEED_TABLES:
        raise ValueError(f"Unknown seed '{seed_key}'")
    meta = SEED_TABLES[seed_key]
    if not db_has_table(meta["table"]):
        return 0

    cols = meta["cols"]; pk = meta["pk"]; table = meta["table"]

    reader = csv.DictReader(file_like)
    if reader.fieldnames:
        reader.fieldnames = [fn.strip() for fn in reader.fieldnames]

    # Header check
    missing, extra = validate_csv_headers(seed_key, reader.fieldnames or [])
    if missing or extra:
        raise ValueError(
            f"Header mismatch. Missing: {missing or '—'}; Extra: {extra or '—'}; "
            f"Expected exactly: {cols}"
        )

    insert_cols  = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    update_set   = ", ".join(f"{c}=excluded.{c}" for c in cols if c != pk)

    sql = text(f"""
        INSERT INTO {table} ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT({pk}) DO UPDATE SET {update_set}
    """)

    written = 0
    try:
        for raw in reader:
            if not raw:
                continue
            row = { (k or "").strip(): v for k, v in raw.items() }
            row = normalize_row(seed_key, row)
            db.session.execute(sql, row)
            written += 1
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return written


# add near the top
from typing import List, Tuple

def validate_csv_headers(seed_key: str, headers: List[str]) -> tuple[list[str], list[str]]:
    meta = SEED_TABLES[seed_key]
    expected = meta["cols"]
    hdrs = [h.strip() for h in (headers or [])]
    missing = [c for c in expected if c not in hdrs]
    extra   = [h for h in hdrs if h not in expected]
    return missing, extra

# app/admin/seed_utils.py



# ---------------- Registry ---------------------------------------------------
'''
SEED_TABLES: dict[str, dict] = {
    "questions": {
        "table": "lca_question",
        "pk": "id",
        "cols": ["id", "number", "text", "title", "caption", "buttons"],
        "title": "Questions",
        "note":  "Looks for seeds/questions.csv",
    },
    "phase_items": {
        "table": "lca_phase_item",
        "pk": "id",
        "cols": ["id", "phase_id", "ordinal", "body", "active"],
        "title": "Phase Items",
        "note":  "Looks for seeds/phase_items.csv",
    },
    "progress_items": {
        "table": "lca_progress_item",
        "pk": "id",
        "cols": ["id", "phase_id", "band", "tone", "body", "ordinal", "active"],
        "title": "Progress Items",
        "note":  "Looks for seeds/progress_items.csv",
    },
    # Add this only if you actually created the table. Otherwise comment it out.
    # "overall_items": {
    #     "table": "lca_overall_item",
    #     "pk": "id",
    #     "cols": ["id", "band", "tone", "body", "ordinal", "active"],
    #     "title": "Overall Items",
    #     "note":  "Looks for seeds/overall_items.csv",
    # },
    "intro_cards": {
        "table": "lca_instruction",
        "pk": "id",
        "cols": ["id", "title", "caption", "content"],
        "title": "Introduction Cards",
        "note":  "Looks for seeds/intro_cards.csv",
    },
    "explain_cards": {
        "table": "lca_explain",
        "pk": "id",
        "cols": ["id", "title", "caption", "content"],
        "title": "Explain Cards",
        "note":  "Looks for seeds/explain_cards.csv",
    },
    # Add this only if you created lca_pause.
    # "pause_cards": {
    #     "table": "lca_pause",
    #     "pk": "id",
    #     "cols": ["id", "title", "caption", "content"],
    #     "title": "Pause Cards",
    #     "note":  "Looks for seeds/pause_cards.csv",
    # },
}

# Backward-compatible aliases (so old tabs/URLs keep working)
SEED_ALIASES = {
    "instruction": "intro_cards",
    "instructions": "intro_cards",
    "intro": "intro_cards",
    "explain": "explain_cards",
    "pause": "pause_cards",
}

def canon_seed(seed: str) -> str:
    s = (seed or "").strip().lower()
    return SEED_ALIASES.get(s, s)
'''
# ---------------- Paths ------------------------------------------------------

def _instance_seeds_dir() -> Path:
    base = Path(current_app.instance_path)
    p = base / "seeds" / "loss"
    p.mkdir(parents=True, exist_ok=True)
    return p

def instance_csv_path(seed: str) -> Path:
    seed = canon_seed(seed)
    return _instance_seeds_dir() / f"{seed}.csv"

# ---------------- DB helpers -------------------------------------------------

def _rows_from_db(seed: str, limit: int | None = None) -> List[Dict]:
    seed = canon_seed(seed)
    meta = SEED_TABLES[seed]
    table = meta["table"]
    cols = meta["cols"]
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'SELECT {col_list} FROM "{table}"'
    if limit:
        sql += " LIMIT :lim"
        res = db.session.execute(text(sql), {"lim": int(limit)})
    else:
        res = db.session.execute(text(sql))
    return [dict(row._mapping) for row in res]

# ---------------- Export / Preview ------------------------------------------

def export_csv(seed: str) -> Response:
    """Return a CSV download of the entire table for this seed."""
    seed = canon_seed(seed)
    if seed not in SEED_TABLES:
        return Response("Unknown seed", 404)
    meta = SEED_TABLES[seed]
    cols = meta["cols"]

    try:
        rows = _rows_from_db(seed, limit=None)
    except SQLAlchemyError as e:
        return Response(f"Export failed: {e}", 404)

    sio = io.StringIO(newline="")
    w = csv.DictWriter(sio, fieldnames=cols)
    w.writeheader()
    for r in rows:
        # ensure only expected columns
        w.writerow({c: r.get(c, "") for c in cols})

    data = sio.getvalue().encode("utf-8")
    fname = f"{seed}.csv"
    headers = {
        "Content-Disposition": f'attachment; filename="{fname}"'
    }
    return Response(data, 200, headers=headers, mimetype="text/csv")

def preview_rows(seed: str, limit: int = 100) -> List[Dict]:
    """First N rows for on-screen preview."""
    seed = canon_seed(seed)
    if seed not in SEED_TABLES:
        return []
    try:
        return _rows_from_db(seed, limit=limit)
    except SQLAlchemyError:
        return []

# ---------------- Upload / Validate -----------------------------------------

def stage_upload_to_instance(seed: str, file_storage) -> Tuple[str, List[str]]:
    """
    Save the uploaded CSV under instance/seeds/loss/<seed>.csv.
    Return (saved_path, headers_list).
    """
    seed = canon_seed(seed)
    path = instance_csv_path(seed)
    # read bytes once
    data = file_storage.read()
    path.write_bytes(data)

    # read header from the staged file
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    return str(path), headers

def validate_csv_headers(seed: str, hdrs: Iterable[str]) -> Tuple[List[str], List[str]]:
    seed = canon_seed(seed)
    exp = SEED_TABLES[seed]["cols"]
    hdrs = [h.strip() for h in (hdrs or [])]
    missing = [c for c in exp if c not in hdrs]
    extra   = [h for h in hdrs if h not in exp]
    return missing, extra

# ---------------- Import -----------------------------------------------------

def _import_rows(seed: str, rows: List[Dict]) -> int:
    """Replace table contents with given rows (simple + deterministic)."""
    seed = canon_seed(seed)
    meta = SEED_TABLES[seed]
    table = meta["table"]
    cols = meta["cols"]

    if not rows:
        return 0

    # Normalize rows to expected columns
    norm = [{c: r.get(c, None) for c in cols} for r in rows]

    col_list = ", ".join(f'"{c}"' for c in cols)
    val_list = ", ".join(f":{c}" for c in cols)
    insert_sql = text(f'INSERT INTO "{table}" ({col_list}) VALUES ({val_list})')

    with db.session.begin():
        db.session.execute(text(f'DELETE FROM "{table}"'))
        db.session.execute(insert_sql, norm)

    return len(norm)

def import_csv_stream(seed: str, file_storage) -> int:
    """
    Import directly from an uploaded FileStorage (used if you want one-step import).
    """
    seed = canon_seed(seed)
    text_stream = io.TextIOWrapper(file_storage.stream, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text_stream)
    rows = list(reader)
    return _import_rows(seed, rows)

def import_from_instance_file(seed: str) -> int:
    """
    Import from the staged file at instance/seeds/loss/<seed>.csv
    (our two-step flow: Upload -> Import).
    """
    seed = canon_seed(seed)
    path = instance_csv_path(seed)
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return _import_rows(seed, rows)

# ---------------- UI meta ----------------------------------------------------

def registry_meta(reg: dict[str, dict]) -> List[dict]:
    """
    Flatten registry for templates. (You can extend this for descriptions etc.)
    """
    out = []
    for key, meta in reg.items():
        out.append({
            "key": key,
            "title": meta.get("title", key),
            "cols": meta.get("cols", []),
            "note": meta.get("note", ""),
        })
    return out
'''
def import_from_repo(seed_key: str) -> int:
    """
    Load a CSV for `seed_key` from the repository and import it.
    Search paths: _REPO_SEED_DIRS.
    Returns the number of imported rows.
    Raises FileNotFoundError if no CSV is found.
    """
    seed_key = canon_seed(seed_key)  # reuse your normalizer
    filename = _DEFAULT_SEED_FILES.get(seed_key, f"{seed_key}.csv")

    # find first existing file
    csv_path = None
    for base in _REPO_SEED_DIRS:
        candidate = base / filename
        if candidate.exists():
            csv_path = candidate
            break

    if not csv_path:
        # Be explicit about where we looked
        tried = [str(p / filename) for p in _REPO_SEED_DIRS]
        raise FileNotFoundError(f"No seed CSV found for '{seed_key}'. Tried: {tried}")

    # stream to the existing importer (which does header validation & DB upsert)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return import_csv_stream(seed_key, f)
'''