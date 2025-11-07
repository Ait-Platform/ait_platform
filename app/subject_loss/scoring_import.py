import os, csv
from pathlib import Path
from flask import current_app
from sqlalchemy import text
from app import db

DEFAULT_CSV = Path(__file__).resolve().parent / "seed" / "lca_scoring_map.csv"
REQUIRED = {"question_id","answer_type","phase_1","phase_2","phase_3","phase_4"}

def load_scoring_map_from_csv(csv_path: str | None = None) -> int:
    path = Path(csv_path or os.getenv("LOSS_CSV", DEFAULT_CSV)).resolve()
    if not path.exists():
        current_app.logger.error("Scoring CSV not found: %s", path)
        return 0

    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        found = set(rdr.fieldnames or [])
        missing = REQUIRED - found
        if missing:
            current_app.logger.error("CSV missing headers: %s. Found: %s", missing, list(found))
            return 0

        # ensure table (remove if you use Alembic)
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS lca_scoring_map (
              question_id INTEGER PRIMARY KEY,
              answer_type TEXT NOT NULL,
              phase_1 INTEGER NOT NULL,
              phase_2 INTEGER NOT NULL,
              phase_3 INTEGER NOT NULL,
              phase_4 INTEGER NOT NULL
            )
        """))
        db.session.execute(text("DELETE FROM lca_scoring_map"))

        n = 0
        for row in rdr:
            db.session.execute(text("""
                INSERT INTO lCA_scoring_map
                (question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
                VALUES (:q, :a, :p1, :p2, :p3, :p4)
            """), {
                "q": int(row["question_id"]),
                "a": row["answer_type"].strip().lower(),
                "p1": int(row["phase_1"]),
                "p2": int(row["phase_2"]),
                "p3": int(row["phase_3"]),
                "p4": int(row["phase_4"]),
            })
            n += 1
        db.session.commit()
        current_app.logger.info("Imported scoring map from %s (%s rows).", path, n)
        return n

def maybe_import_on_boot():
    if os.getenv("LOSS_IMPORT_ON_BOOT", "0") == "1":
        load_scoring_map_from_csv()
