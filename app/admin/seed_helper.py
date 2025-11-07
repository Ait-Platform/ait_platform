# app/admin/seed_helper.py
from __future__ import annotations
from io import StringIO
import csv
from typing import Dict, Type, Tuple, List
from flask import make_response, abort
from sqlalchemy import select
from app.extensions import db
import io, os
from werkzeug.datastructures import FileStorage
from pathlib import Path
from typing import Iterable

from flask import current_app

from app.models.loss import (
    LcaQuestion, LcaPhaseItem, LcaProgressItem,
    LcaInstruction, LcaExplain, LcaOverallItem,
    LcaPause,
)
from app.seed.seed_simple import SEEDS

# -------- generic utils --------
def columns_for(model) -> List[str]:
    """Return DB column names for a model (no relationships)."""
    return [c.name for c in model.__table__.columns]

def _boolify(v):
    if v is None: return None
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")

def _coerce(model, colname: str, raw):
    if raw is None or raw == "":
        return None
    col = model.__table__.columns.get(colname)
    if not col:
        return raw
    try:
        py = getattr(col.type, "python_type", str)
        if py is bool:
            return _boolify(raw)
        return py(raw)
    except Exception:
        tname = col.type.__class__.__name__.lower()
        if tname.startswith("integer"):
            try: return int(raw)
            except Exception: return None
        if tname.startswith("float") or tname.startswith("numeric"):
            try: return float(raw)
            except Exception: return None
        if tname.startswith("boolean"):
            return _boolify(raw)
        return raw

# -------- core helpers you call from routes --------
def seed_preview_rows(model):
    cols = columns_for(model)
    objs = db.session.execute(select(model)).scalars().all()
    # return list[dict] instead of model objects
    rows = [{c: getattr(o, c) for c in cols} for o in objs]
    return cols, rows

def seed_export_csv_response(model, filename: str):
    """Return a Flask Response with CSV of all rows for the model."""
    cols = columns_for(model)
    sio = StringIO()
    writer = csv.DictWriter(sio, fieldnames=cols)
    writer.writeheader()
    for obj in db.session.execute(select(model)).scalars():
        writer.writerow({c: getattr(obj, c) for c in cols})
    resp = make_response(sio.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

def seed_import_csv_stream(model, file_storage) -> Tuple[int, int, int]:
    """
    Upsert rows from a CSV FileStorage into model by id (or 'code' if present).
    Returns (added, updated, skipped).
    """
    if not file_storage or not file_storage.filename.lower().endswith(".csv"):
        abort(400, description="Please upload a .csv file.")

    cols = set(columns_for(model))
    data = file_storage.stream.read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(StringIO(data))
    added = updated = skipped = 0

    for r in reader:
        obj = None
        # lookup by id first
        if r.get("id"):
            try:
                obj = db.session.get(model, int(r["id"]))
            except Exception:
                obj = None
        # fallback: by 'code' if that column exists
        if obj is None and "code" in cols and r.get("code"):
            obj = db.session.execute(select(model).where(getattr(model, "code") == r["code"])).scalars().first()

        created = False
        if obj is None:
            obj = model()
            created = True

        # set only known columns
        for k, v in r.items():
            if k not in cols:
                continue
            setattr(obj, k, _coerce(model, k, v))

        db.session.add(obj)
        added += 1 if created else 0
        updated += 0 if created else 1

    db.session.commit()
    return added, updated, skipped

def registry_meta(registry: Dict[str, Type]):
    """Return list of dicts with seed key, title, and columns for UI."""
    meta = []
    for key, model in registry.items():
        meta.append({
            "key": key,
            "title": key.replace("_", " ").title(),
            "columns": columns_for(model),
        })
    return meta

def seed_import_csv_path(model, path) -> tuple[int, int, int]:
    """Upsert rows from a CSV file on disk by reusing the stream importer."""
    with open(path, "rb") as fh:
        data = fh.read()
    fs = FileStorage(stream=io.BytesIO(data), filename=os.path.basename(path), content_type="text/csv")
    return seed_import_csv_stream(model, fs)


'''
def fetch_rows(seed_or_meta: str | dict) -> list[dict]:
    """Generic DB → list[dict] with configured columns in order."""
    meta = SEED_CFG[seed_or_meta] if isinstance(seed_or_meta, str) else seed_or_meta
    model = meta["model"]
    cols: list[str] = meta["cols"]
    q = db.session.query(*[getattr(model, c) for c in cols])
    out: list[dict] = []
    for row in q.all():
        # row is a SQLAlchemy Row/tuple; zip into a dict by our columns
        out.append(dict(zip(cols, row)))
    return out
'''


SEED_CFG: dict[str, dict] = {
    "questions": {
        "title": "Questions",
        "model": LcaQuestion,
        "cols": ["id", "number", "text", "title", "caption", "buttons"],
        "filename": "questions.csv",
    },
    "phase_items": {
        "title": "Phase Items",
        "model": LcaPhaseItem,
        "cols": ["id", "phase_id", "ordinal", "body", "active"],
        "filename": "phase_items.csv",
    },
    "progress_items": {
        "title": "Progress Items",
        "model": LcaProgressItem,
        "cols": ["id", "phase_id", "band", "tone", "body", "ordinal", "active"],
        "filename": "progress_items.csv",
    },
    "instruction": {
        "title": "Introduction Cards",
        "model": LcaInstruction,
        "cols": ["id", "title", "caption", "content"],
        "filename": "instruction.csv",
    },
    "explain": {
        "title": "Explain Cards",
        "model": LcaExplain,
        "cols": ["id", "title", "caption", "content"],
        "filename": "explain.csv",
    },
        "overall_items": {
        "title": "Overall Items",
        "model": LcaOverallItem,
        "cols":  ["id","band","type","label","key_need","body","ordinal","active"],
        "filename": "overall_items.csv",
    },

    "pause": {
        "title": "Pause Cards",
        "model": LcaPause,
        "cols":  ["id","title","caption","content"],
        "filename": "pause.csv",
    },
}



def _seed_keys():
    # Keep a stable order you like
    order = ["questions", "phase_items", "progress_items",
             "overall_items", "instruction", "explain", "pause"]
    return [k for k in order if k in SEED_CFG]


def seed_root() -> Path:
    root = Path(current_app.instance_path) / "seeds" / "loss"
    root.mkdir(parents=True, exist_ok=True)
    return root

def seed_csv_path(seed: str) -> Path:
    meta = SEED_CFG.get(seed, {})
    filename = meta.get("filename", f"{seed}.csv")
    return seed_root() / filename
'''
def fetch_rows(meta: dict) -> list[dict]:
    """Return rows as list of dicts with exactly meta['cols'] keys."""
    Model = meta["model"]
    cols = meta["cols"]
    q = db.session.query(Model).order_by(Model.id.asc())
    out: list[dict] = []
    for row in q.all():
        d = {}
        for c in cols:
            d[c] = getattr(row, c, "")
        out.append(d)
    return out
'''
def mget(meta, key, default=None):
    """Works for both dict-like and object-like meta."""
    if isinstance(meta, dict):
        return meta.get(key, default)
    return getattr(meta, key, default)

def mset(meta, key, value):
    """Set on dict or object; ignore if not possible."""
    try:
        if isinstance(meta, dict):
            meta[key] = value
        else:
            setattr(meta, key, value)
    except Exception:
        pass

import csv
from pathlib import Path
from flask import send_file, url_for, redirect, flash

SEED_DIR = Path(__file__).resolve().parents[3] / "seed"

def write_csv(seed, headers, dict_rows):
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    p = SEED_DIR / f"{seed}.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(dict_rows)
    return p

def read_csv(seed):
    p = SEED_DIR / f"{seed}.csv"
    if not p.exists():
        return p, [], []
    with p.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
        headers = r.fieldnames or []
    return p, headers, rows


def _resolve_cols(meta):
    """
    Return a list of column names for a SeedMeta-like object.
    Prefers: meta.cols -> meta.columns -> model.__table__.columns -> sample dict keys.
    Also normalizes comma-separated strings into a list.
    """
    # 1) Try explicit attributes
    cols = getattr(meta, "cols", None)
    if cols is None:
        cols = getattr(meta, "columns", None)

    # 2) Normalize a comma-separated string to list
    if isinstance(cols, str):
        cols = [c.strip() for c in cols.split(",") if c.strip()]

    # 3) Infer from SQLAlchemy model if still empty/None
    if not cols:
        model = getattr(meta, "model", None)
        if model is not None and hasattr(model, "__table__"):
            try:
                cols = [c.name for c in model.__table__.columns]
            except Exception:
                cols = None

    # 4) Infer from a sample mapping if present
    if not cols:
        sample = getattr(meta, "sample", None)
        if isinstance(sample, dict):
            cols = list(sample.keys())

    # 5) Final fallback
    if not cols:
        cols = []

    # Backfill for consistency so later code can rely on meta.cols
    try:
        setattr(meta, "cols", cols)
    except Exception:
        pass

    return cols

# --- helpers: put near the top of routes.py ---
import csv
from pathlib import Path
from flask import current_app, request, url_for, redirect, flash, send_file, abort

def get_seed_dir() -> Path:
    # .../ait_platform/app -> parent is .../ait_platform
    return Path(current_app.root_path).parent / "seed"

def normalize_cols(meta):
    raw = getattr(meta, "cols", None) or getattr(meta, "columns", None)
    if isinstance(raw, str):
        raw = [c.strip() for c in raw.split(",") if c.strip()]
    cols = []
    if raw:
        for c in raw:
            if isinstance(c, (tuple, list)) and c:
                attr = str(c[0]); label = str(c[1]) if len(c) > 1 else attr
            elif isinstance(c, dict):
                attr = str(c.get("key") or c.get("name") or c.get("field") or c.get("attr") or "")
                label = str(c.get("label") or c.get("heading") or attr)
            else:
                attr = str(c); label = attr
            if attr:
                cols.append((attr, label))
    if not cols and getattr(meta, "model", None):
        cols = [(c.name, c.name) for c in meta.model.__table__.columns]
    return cols

def map_rows_from_db(rows, cols):
    out = []
    for r in rows or []:
        m = {}
        for attr, label in cols:
            m[label] = (r.get(attr, "") if isinstance(r, dict) else getattr(r, attr, ""))
        out.append(m)
    return out

def write_csv(seed: str, headers, dict_rows):
    d = get_seed_dir()
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{seed}.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(headers))
        w.writeheader()
        w.writerows(dict_rows)
    return p

def read_csv(seed: str):
    p = get_seed_dir() / f"{seed}.csv"
    if not p.exists():
        return p, [], []
    with p.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
        headers = r.fieldnames or []
    return p, headers, rows

def best_loss_back_url():
    # Prefer a LOSS-specific admin endpoint; allow ?back=/... to override
    custom = request.args.get("back")
    if custom:
        return custom
    for ep in [
        "admin_bp.loss_admin_home",
        "admin_bp.loss_admin",
        "admin_bp.loss_dashboard",
        "admin_bp.loss_home",
        "admin_bp.loss_index",
    ]:
        try:
            return url_for(ep)
        except Exception:
            continue
    return url_for("admin_bp.admin_home")  # last resort

# --- add these near your other helpers ---
def resolve_seed_key(seed: str) -> str:
    """Map 'instruction' <-> 'instructions', ignore case, or 404."""
    if seed in SEEDS:
        return seed
    low_map = {k.lower(): k for k in SEEDS.keys()}
    if seed.lower() in low_map:
        return low_map[seed.lower()]
    # simple plural/singular heuristics
    if (seed + "s") in SEEDS:
        return seed + "s"
    if seed.endswith("s") and seed[:-1] in SEEDS:
        return seed[:-1]
    abort(404, f"Unknown seed '{seed}'")
'''
def db_rows_fallback(seed: str, meta):
    """Use fetch_rows(seed) else fallback to model.query.all() when bound."""
    rows = fetch_rows(seed)
    if rows is None and getattr(meta, "model", None):
        try:
            return db.session.query(meta.model).all()
        except Exception:
            return []
    return rows or []
'''
# app/admin/seed_helper.py

from typing import Any, Optional, Tuple
from flask import abort
from importlib import import_module

# Expect SEEDS to be importable here; if defined elsewhere, import it:
# from .whatever_defines_seeds import SEEDS

def _resolve_seed_key(seed: str) -> str:
    """Return the canonical SEEDS key (handles case and simple plural/singular)."""
    if seed in SEEDS:
        return seed
    low_map = {k.lower(): k for k in SEEDS.keys()}
    s = str(seed).strip()
    if s.lower() in low_map:
        return low_map[s.lower()]
    if (s + "s") in SEEDS:
        return s + "s"
    if s.endswith("s") and s[:-1] in SEEDS:
        return s[:-1]
    abort(404, f"Unknown seed '{seed}'")

def _resolve_meta(seed_or_meta: Any) -> Any:
    """
    Accepts a seed key or a meta object. Returns a SeedMeta-like object (or dict).
    Handles string aliases: SEEDS['instructions'] = 'questions' etc.
    """
    meta = seed_or_meta
    if isinstance(meta, str) or meta is None:
        key = _resolve_seed_key(meta or "")
        meta = SEEDS.get(key)
        # Some setups use string aliases: e.g. 'instructions' -> 'questions'
        if isinstance(meta, str):
            alias_key = _resolve_seed_key(meta)
            meta = SEEDS.get(alias_key)
    return meta

def _resolve_model(model_ref: Any):
    """
    Normalize model reference into an actual SQLAlchemy model class.
    Accepts a class or a string like 'LcaQuestion'. Tries app.models.loss first.
    """
    if model_ref is None:
        return None
    if isinstance(model_ref, type):
        return model_ref  # already a class
    if isinstance(model_ref, str):
        # Try common module first
        for mod_path in ("app.models.loss", "app.models", "models"):
            try:
                mod = import_module(mod_path)
                if hasattr(mod, model_ref):
                    return getattr(mod, model_ref)
            except Exception:
                continue
    return None

def fetch_rows(seed_or_meta: Any, limit: Optional[int] = None):
    """
    Return rows for a seed. Works if you pass a seed key or a meta object.
    Robust to SEEDS values that are strings (aliases) or dict/object metainfo.
    """
    meta = _resolve_meta(seed_or_meta)

    # Pull a model reference from object or dict
    model_ref = getattr(meta, "model", None)
    if model_ref is None and isinstance(meta, dict):
        model_ref = meta.get("model")

    Model = _resolve_model(model_ref)
    if Model is None:
        # No bound model; nothing to fetch
        return []

    q = db.session.query(Model)
    if limit:
        q = q.limit(limit)
    return q.all()

def db_rows_fallback(seed: str, meta: Any):
    """
    Try fetch_rows(seed). If still empty and model exists, do a direct query.
    """
    try:
        rows = fetch_rows(seed)
    except Exception:
        rows = []

    if rows:
        return rows

    # Try direct query off meta.model
    model_ref = getattr(meta, "model", None)
    if model_ref is None and isinstance(meta, dict):
        model_ref = meta.get("model")

    Model = _resolve_model(model_ref)
    if Model is None:
        return []

    try:
        return db.session.query(Model).all()
    except Exception:
        return []
