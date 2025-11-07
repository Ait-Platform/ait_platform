# app/school_loss/seed_cli.py
from __future__ import annotations
import csv, os
from pathlib import Path
from flask import current_app
from sqlalchemy import text
from app import db

SEED_DIR = Path(__file__).resolve().parent / "seed"
PHASES_CSV        = SEED_DIR / "lca_phase.csv"
PHASE_ITEMS_CSV   = SEED_DIR / "lca_phase_item.csv"
SCORING_MAP_CSV   = SEED_DIR / "lca_scoring_map.csv"

def _truthy(v) -> int:
    s = str(v).strip().lower()
    if s in {"1","true","t","yes","y","on"}:  return 1
    if s in {"0","false","f","no","n","off"}: return 0
    try:
        return 1 if int(s) != 0 else 0
    except Exception:
        return 1

def import_phases_from_csv(csv_path: str | Path = PHASES_CSV) -> int:
    path = Path(csv_path)
    if not path.exists():
        current_app.logger.error("Phases CSV not found: %s", path)
        return 0

    # ensure table exists (id PK + fields you use)
    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS lca_phase (
          id              INTEGER PRIMARY KEY,
          order_index     INTEGER NOT NULL,
          name            TEXT    NOT NULL,
          max_points      INTEGER NOT NULL,
          points_per_item INTEGER NOT NULL,
          high_is_positive INTEGER NOT NULL,
          neutral_line    TEXT    NOT NULL,
          active          INTEGER NOT NULL
        )
    """))

    n = 0
    with path.open(newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        need = {"id","order_index","name","max_points","points_per_item",
                "high_is_positive","neutral_line","active"}
        found = set(rdr.fieldnames or [])
        missing = need - found
        if missing:
            current_app.logger.error("Phases CSV missing headers: %s. Found: %s", missing, list(found))
            return 0

        for row in rdr:
            db.session.execute(text("""
                INSERT INTO lca_phase
                (id, order_index, name, max_points, points_per_item, high_is_positive, neutral_line, active)
                VALUES (:id,:oi,:name,:max,:ppi,:hip,:nl,:act)
                ON CONFLICT(id) DO UPDATE SET
                  order_index=excluded.order_index,
                  name=excluded.name,
                  max_points=excluded.max_points,
                  points_per_item=excluded.points_per_item,
                  high_is_positive=excluded.high_is_positive,
                  neutral_line=excluded.neutral_line,
                  active=excluded.active
            """), {
                "id":   int(row["id"]),
                "oi":   int(row["order_index"]),
                "name": (row["name"] or "").strip(),
                "max":  int(row["max_points"]),
                "ppi":  int(row["points_per_item"]),
                "hip":  _truthy(row["high_is_positive"]),
                "nl":   (row.get("neutral_line") or "No notable markers in this phase.").strip(),
                "act":  _truthy(row["active"]),
            })
            n += 1
    db.session.commit()
    current_app.logger.info("Imported/updated %s lca_phase rows.", n)
    return n

def import_phase_items_from_csv(csv_path: str | Path = PHASE_ITEMS_CSV) -> tuple[int,int]:
    path = Path(csv_path)
    if not path.exists():
        current_app.logger.error("Phase items CSV not found: %s", path)
        return (0,0)

    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS lca_phase_item (
          phase_id INTEGER NOT NULL,
          ordinal  INTEGER NOT NULL,
          body     TEXT    NOT NULL,
          active   INTEGER NOT NULL
        )
    """))

    imported, skipped = 0, 0
    rows = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        need = {"phase_id","ordinal","body","active"}
        found = set(rdr.fieldnames or [])
        missing = need - found
        if missing:
            current_app.logger.error("Phase items CSV missing headers: %s. Found: %s", missing, list(found))
            return (0,0)

        for r in rdr:
            pid = (r.get("phase_id") or "").strip()
            ord_ = (r.get("ordinal") or "").strip()
            if not pid.isdigit() or not ord_.isdigit():
                skipped += 1
                continue
            rows.append({
                "phase_id": int(pid),
                "ordinal":  int(ord_),
                "body":     (r.get("body") or "").strip(),
                "active":   _truthy(r.get("active", 1)),
            })
            imported += 1

    db.session.execute(text("DELETE FROM lca_phase_item"))
    for r in rows:
        db.session.execute(text("""
            INSERT INTO lca_phase_item (phase_id, ordinal, body, active)
            VALUES (:phase_id, :ordinal, :body, :active)
        """), r)
    db.session.commit()
    current_app.logger.info("Imported %s lca_phase_item rows (skipped %s).", imported, skipped)
    return (imported, skipped)

def import_scoring_map_from_csv(csv_path: str | Path = SCORING_MAP_CSV) -> int:
    path = Path(csv_path)
    if not path.exists():
        current_app.logger.error("Scoring map CSV not found: %s", path)
        return 0

    db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS lca_scoring_map (
          question_id INTEGER PRIMARY KEY,
          answer_type TEXT    NOT NULL,
          phase_1     INTEGER NOT NULL,
          phase_2     INTEGER NOT NULL,
          phase_3     INTEGER NOT NULL,
          phase_4     INTEGER NOT NULL
        )
    """))

    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        need = {"question_id","answer_type","phase_1","phase_2","phase_3","phase_4"}
        found = set(rdr.fieldnames or [])
        missing = need - found
        if missing:
            current_app.logger.error("Scoring CSV missing headers: %s. Found: %s", missing, list(found))
            return 0

        db.session.execute(text("DELETE FROM lca_scoring_map"))
        n = 0
        for row in rdr:
            db.session.execute(text("""
                INSERT INTO lca_scoring_map
                (question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
                VALUES (:q, :a, :p1, :p2, :p3, :p4)
            """), {
                "q":  int(row["question_id"]),
                "a":  (row["answer_type"] or "").strip().lower(),
                "p1": int(row["phase_1"]),
                "p2": int(row["phase_2"]),
                "p3": int(row["phase_3"]),
                "p4": int(row["phase_4"]),
            })
            n += 1
    db.session.commit()
    current_app.logger.info("Imported %s lca_scoring_map rows.", n)
    return n
'''
def recompute_phase_maxima_sql() -> dict[int,int]:
    row = db.session.execute(text("""
        WITH q AS (
          SELECT question_id,
                 MAX(phase_1) AS p1, MAX(phase_2) AS p2,
                 MAX(phase_3) AS p3, MAX(phase_4) AS p4
          FROM lca_scoring_map
          GROUP BY question_id
        )
        SELECT COALESCE(SUM(p1),0),
               COALESCE(SUM(p2),0),
               COALESCE(SUM(p3),0),
               COALESCE(SUM(p4),0)
        FROM q
    """)).first()
    maxes = {1:int(row[0] or 0), 2:int(row[1] or 0), 3:int(row[2] or 0), 4:int(row[3] or 0)}
    for idx, val in maxes.items():
        db.session.execute(text("""
            UPDATE lca_phase SET max_points=:v
            WHERE order_index=:idx AND COALESCE(active,1)=1
        """), {"v": val, "idx": idx})
    db.session.commit()
    current_app.logger.info("Updated lca_phase.max_points: %s", maxes)
    return maxes
'''