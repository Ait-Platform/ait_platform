# scripts/cleanup_duplicates.py
"""
Canonicalizes duplicate auth tables in SQLite:
- user / auth_user
- auth_payment_log / auth_paymentlog
- auth_approved_admin / auth_approved_admins (+ approved_admins view)

Safe to run multiple times (idempotent).
"""

from collections import defaultdict
from typing import List, Dict, Any, Tuple

from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError

# --- import your app context ---
try:
    # If your factory is create_app()
    from app import create_app, db  # type: ignore
    app = create_app()
except Exception:
    # Fallback: if your app initializes db at import-time
    from app.extensions import db  # type: ignore
    from flask import Flask
    app = Flask(__name__)

def table_exists(insp, name: str) -> bool:
    try:
        return insp.has_table(name)
    except Exception:
        return False

def get_cols(insp, name: str) -> List[str]:
    try:
        return [c["name"] for c in insp.get_columns(name)]
    except Exception:
        return []

def intersect(a: List[str], b: List[str]) -> List[str]:
    aset = {x.lower(): x for x in a}
    bset = {x.lower(): x for x in b}
    out = []
    for k in aset:
        if k in bset:
            out.append(aset[k])  # preserve canonical casing from first list
    return out

def ensure_column(con, insp, table: str, name: str, ddl_type: str, default_sql: str = None):
    cols = {c.lower() for c in get_cols(insp, table)}
    if name.lower() not in cols:
        con.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl_type}'))
        if default_sql is not None:
            con.execute(text(f'UPDATE {table} SET {name} = {default_sql} WHERE {name} IS NULL'))

def rows(con, sql: str, params=None) -> List[Dict[str, Any]]:
    return [dict(r) for r in con.execute(text(sql), params or {}).mappings().all()]

def upsert_merge(con, legacy: str, canon: str, uniq_cols: List[str]) -> Tuple[int, int]:
    """Copy rows from legacy -> canon for intersecting columns; skip if uniq key exists."""
    insp = inspect(con)
    if not table_exists(insp, legacy) or not table_exists(insp, canon):
        return (0, 0)

    lcols = get_cols(insp, legacy)
    ccols = get_cols(insp, canon)
    cols = intersect(lcols, ccols)
    if not cols:
        return (0, 0)

    sel = ", ".join(cols)
    legacy_rows = rows(con, f"SELECT {sel} FROM {legacy}")
    inserted = 0
    skipped = 0
    for r in legacy_rows:
        where_parts = []
        params = {}
        for k in uniq_cols:
            if k in cols and k in r:
                where_parts.append(f"lower({k}) = lower(:{k})")
                params[k] = r[k]
        exists = False
        if where_parts:
            where = " AND ".join(where_parts)
            exists = con.execute(text(f"SELECT 1 FROM {canon} WHERE {where} LIMIT 1"), params).fetchone() is not None
        if exists:
            skipped += 1
            continue

        placeholders = ", ".join([f":{c}" for c in cols])
        con.execute(text(f"INSERT INTO {canon} ({sel}) VALUES ({placeholders})"), r)
        inserted += 1
    return (inserted, skipped)

def dedupe_user(con) -> int:
    """Keep earliest id per lower(email)."""
    if not table_exists(inspect(con), '"user"'):
        return 0
    dupes = rows(con, """
        SELECT lower(email) AS k, COUNT(*) AS n
        FROM "user"
        WHERE email IS NOT NULL AND email <> ''
        GROUP BY lower(email)
        HAVING COUNT(*) > 1
    """)
    to_delete = []
    for d in dupes:
        k = d["k"]
        ids = [r["id"] for r in rows(con, 'SELECT id FROM "user" WHERE lower(email)=:k ORDER BY id', {"k": k})]
        to_delete.extend(ids[1:])  # keep first
    deleted = 0
    for _id in to_delete:
        con.execute(text('DELETE FROM "user" WHERE id=:id'), {"id": _id})
        deleted += 1
    return deleted

def choose_payment_key(insp, table: str) -> List[str]:
    cols = {c.lower() for c in get_cols(insp, table)}
    if {"provider", "order_id"} <= cols:
        return ["provider", "order_id"]
    if "external_id" in cols:
        return ["external_id"]
    if {"gateway", "order_id"} <= cols:
        return ["gateway", "order_id"]
    # fallback: very weak (may not dedupe well)
    if "idempotency_key" in cols:
        return ["idempotency_key"]
    return []

def dedupe_payment(con) -> int:
    insp = inspect(con)
    t = "auth_payment_log"
    if not table_exists(insp, t):
        return 0
    key = choose_payment_key(insp, t)
    if not key:
        return 0

    # Build a unique signature per row
    key_expr = " || '|' || ".join([f"COALESCE(CAST({k} AS TEXT),'')" for k in key])
    dups = rows(con, f"""
        WITH sig AS (
          SELECT id, {key_expr} AS s
          FROM {t}
        )
        SELECT s, COUNT(*) AS n
        FROM sig
        GROUP BY s
        HAVING COUNT(*) > 1
    """)
    to_delete = []
    for d in dups:
        s = d["s"]
        ids = [r["id"] for r in rows(con, f"""
            WITH sig AS (
              SELECT id, {key_expr} AS s FROM {t}
            )
            SELECT p.id
            FROM {t} p
            JOIN sig ON sig.id = p.id
            WHERE sig.s = :s
            ORDER BY p.id
        """, {"s": s})]
        to_delete.extend(ids[1:])
    deleted = 0
    for _id in to_delete:
        con.execute(text(f"DELETE FROM {t} WHERE id=:id"), {"id": _id})
        deleted += 1
    return deleted

def dedupe_admin(con) -> int:
    insp = inspect(con)
    t = "auth_approved_admin"
    if not table_exists(insp, t):
        return 0
    # ensure columns so view works reliably
    ensure_column(con, insp, t, "subject", "TEXT")
    ensure_column(con, insp, t, "active", "INTEGER", default_sql="1")

    dups = rows(con, f"""
        SELECT lower(email) AS e, lower(COALESCE(subject,'')) AS s, COUNT(*) AS n
        FROM {t}
        GROUP BY lower(email), lower(COALESCE(subject,''))
        HAVING COUNT(*) > 1
    """)
    to_delete = []
    for d in dups:
        e, s = d["e"], d["s"]
        ids = [r["id"] for r in rows(con, f"""
            SELECT id
            FROM {t}
            WHERE lower(email)=:e AND lower(COALESCE(subject,''))=:s
            ORDER BY id
        """, {"e": e, "s": s})]
        to_delete.extend(ids[1:])
    deleted = 0
    for _id in to_delete:
        con.execute(text(f"DELETE FROM {t} WHERE id=:id"), {"id": _id})
        deleted += 1
    return deleted

def ensure_indexes(con):
    # users
    con.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS idx_user_email ON "user"(email)'))
    # payments (best-effort
