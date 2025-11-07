# app/admin/seed_simple.py
from dataclasses import dataclass
from typing import List, Tuple, Any, Dict

# ⚠️ Adjust these imports to match your models module paths
# app/seed/seed_simple.py

# ⬇️ replace the old "from app.models import (...)" with this:
try:
    # if your models are re-exported from app.models (some projects do)
    from app.models.loss import (
        LcaQuestion,
        LcaPhaseItem,
        LcaProgressItem,
        LcaInstruction,
        LcaExplain,
    )
except ImportError:
    # fallback to the real module where they are defined
    from app.models.loss import (
        LcaQuestion,
        LcaPhaseItem,
        LcaProgressItem,
        LcaInstruction,
        LcaExplain,
    )


@dataclass
class SeedMeta:
    title: str
    model: Any
    columns: List[Tuple[str, str]]  # [(name, type)] type in {"str","int","bool","text"}
    order_by: List[str] | None = None
    pk: str = "id"

# Keep only what you actually use
SEEDS: Dict[str, SeedMeta] = {
    "questions": SeedMeta(
        title="Questions",
        model=LcaQuestion,
        columns=[
            ("id", "int"),      # existing IDs editable is optional; keep for visibility
            ("number", "int"),
            ("title", "str"),
            ("caption", "text"),
            ("text", "text"),
            ("buttons", "str"),
        ],
        order_by=["number", "id"],
    ),
    "phase-items": SeedMeta(
        title="Phase Items",
        model=LcaPhaseItem,
        columns=[
            ("id", "int"),
            ("phase_id", "int"),
            ("ordinal", "int"),
            ("body", "text"),
            ("active", "bool"),
        ],
        order_by=["phase_id", "ordinal", "id"],
    ),
    "progress-items": SeedMeta(
        title="Progress Items",
        model=LcaProgressItem,
        columns=[
            ("id", "int"),
            ("phase_id", "int"),
            ("band", "str"),      # low | mid | high
            ("tone", "str"),      # positive | slightly_positive | negative
            ("ordinal", "int"),
            ("body", "text"),
            ("active", "bool"),
        ],
        order_by=["phase_id", "band", "tone", "ordinal", "id"],
    ),
    "instruction": SeedMeta(
        title="Instruction Cards",
        model=LcaInstruction,
        columns=[
            ("id", "int"),
            ("title", "str"),
            ("caption", "text"),
            ("content", "text"),
        ],
        order_by=["id"],
    ),
    "explain": SeedMeta(
        title="Explain Cards",
        model=LcaExplain,
        columns=[
            ("id", "int"),
            ("title", "str"),
            ("caption", "text"),
            ("content", "text"),
        ],
        order_by=["id"],
    ),
    # "overall-items": SeedMeta(...),  # add if/when you have a model
}

def _coerce(val: str, typ: str):
    if typ == "int":
        return int(val) if str(val).strip() != "" else None
    if typ == "bool":
        # checkboxes come as "on" when present; missing means False
        return str(val).lower() in ("1", "true", "on", "yes")
    # str/text
    return val or ""

def _order_query(query, meta: SeedMeta):
    if not meta.order_by:
        return query
    cols = []
    for name in meta.order_by:
        cols.append(getattr(meta.model, name))
    return query.order_by(*cols)

def fetch_rows(seed: str):
    """Read all rows for a seed."""
    from app import db  # import inside to avoid circular imports
    meta = SEEDS[seed]
    return _order_query(meta.model.query, meta).all()

def save_from_form(seed: str, form) -> dict:
    """
    Apply edits from a single form post.
    Field names:
      - existing rows:  row-<id>-<col>
      - delete flags:   del-<id>=on
      - new rows:       new-<n>-<col>  (n = 1..N)
    """
    from app import db  # late import prevents circulars
    meta = SEEDS[seed]
    Model = meta.model

    bool_cols = {c for c, t in meta.columns if t == "bool"}

    # group existing rows
    existing: Dict[str, Dict[str, Any]] = {}
    for k, v in form.items():
        if k.startswith("row-"):
            _, id_str, col = k.split("-", 2)
            existing.setdefault(id_str, {})[col] = v

    to_delete = set()
    for k in form.keys():
        if k.startswith("del-"):
            _, id_str = k.split("-", 1)
            to_delete.add(id_str)

    # group new rows
    new_rows: Dict[str, Dict[str, Any]] = {}
    for k, v in form.items():
        if k.startswith("new-"):
            _, n, col = k.split("-", 2)
            new_rows.setdefault(n, {})[col] = v

    updated = 0
    created = 0
    deleted = 0

    # existing rows
    for id_str, data in existing.items():
        rid = int(id_str)
        obj = Model.query.get(rid)
        if not obj:
            continue

        if id_str in to_delete:
            db.session.delete(obj)
            deleted += 1
            continue

        # ensure missing checkboxes become False
        for col, typ in meta.columns:
            if typ == "bool" and col not in data:
                data[col] = "off"

        for col, typ in meta.columns:
            if col == meta.pk:
                continue
            if col in data:
                setattr(obj, col, _coerce(data[col], typ))

        updated += 1

    # new rows
    for n, data in new_rows.items():
        # consider row valid if any editable column (excluding id/pk) is non-empty
        meaningful = False
        for col, typ in meta.columns:
            if col == meta.pk:
                continue
            if typ == "bool":
                # if checkbox missing, treat as False
                data.setdefault(col, "off")
            if str(data.get(col, "")).strip():
                meaningful = True
        if not meaningful:
            continue

        obj = Model()
        for col, typ in meta.columns:
            if col == meta.pk:
                continue
            if col in data or typ == "bool":
                setattr(obj, col, _coerce(data.get(col), typ))
        db.session.add(obj)
        created += 1

    db.session.commit()
    return {"updated": updated, "created": created, "deleted": deleted}

