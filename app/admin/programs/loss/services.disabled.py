# loss/services.py
from datetime import date, datetime
from sqlalchemy import text
from app.extensions import db
# app/services/loss_runs.py
from types import SimpleNamespace
from sqlalchemy import select, func, desc
from app.models.loss import LcaResult


# 1) Materialize lca_scorecard_run from latest responses + scorecard_v
#    - Joins view by question_id (works even if view lacks run_id)
#    - Accepts either view schema: phase_1..phase_4 OR p1..p4 (handled via COALESCE)
SQL_MATERIALIZE_SCORECARD_RUN = text("""
WITH last_resp AS (
  SELECT r.*
  FROM lca_response r
  JOIN (
    SELECT run_id, user_id, question_id, MAX(id) AS max_id
    FROM lca_response
    WHERE run_id = :run_id
    GROUP BY run_id, user_id, question_id
  ) mx ON r.id = mx.max_id
)
INSERT OR REPLACE INTO lca_scorecard_run
  (run_id, user_id, question_id, phase_1, phase_2, phase_3, phase_4, answer, score_total)
SELECT
  lr.run_id,
  lr.user_id,
  lr.question_id,
  COALESCE(v.phase_1, v.p1, 0),
  COALESCE(v.phase_2, v.p2, 0),
  COALESCE(v.phase_3, v.p3, 0),
  COALESCE(v.phase_4, v.p4, 0),
  lr.answer,
  COALESCE(v.phase_1, v.p1, 0)
+ COALESCE(v.phase_2, v.p2, 0)
+ COALESCE(v.phase_3, v.p3, 0)
+ COALESCE(v.phase_4, v.p4, 0)
FROM last_resp lr
LEFT JOIN lca_scorecard_v v
       ON v.question_id = lr.question_id
""")

# 2) Clear any existing lca_result row for this run (older SQLite, no UPSERT)
SQL_DELETE_RESULT_FOR_RUN = text("""
DELETE FROM lca_result WHERE run_id = :run_id
""")

# 3) Insert totals from lca_scorecard_run
SQL_INSERT_RESULT_FROM_SCORECARD_RUN = text("""
INSERT INTO lca_result
  (user_id, phase_1, phase_2, phase_3, phase_4, total, run_id, subject)
SELECT
  s.user_id,
  SUM(s.phase_1) AS phase_1,
  SUM(s.phase_2) AS phase_2,
  SUM(s.phase_3) AS phase_3,
  SUM(s.phase_4) AS phase_4,
  (SUM(s.phase_1)+SUM(s.phase_2)+SUM(s.phase_3)+SUM(s.phase_4)) AS total,
  :run_id AS run_id,
  'LOSS'  AS subject
FROM lca_scorecard_run s
WHERE s.run_id = :run_id
GROUP BY s.user_id
""")

# 4) Update max columns in lca_result using the questions that actually appeared in the run
#    (max == all weights summed from lca_question_phase_map for those questions)
SQL_UPDATE_RESULT_MAX_COLUMNS = text("""
WITH qset AS (
  SELECT DISTINCT question_id
  FROM lca_scorecard_run
  WHERE run_id = :run_id
),
mx AS (
  SELECT
    SUM(COALESCE(m.phase_1,0)) AS max_p1,
    SUM(COALESCE(m.phase_2,0)) AS max_p2,
    SUM(COALESCE(m.phase_3,0)) AS max_p3,
    SUM(COALESCE(m.phase_4,0)) AS max_p4
  FROM lca_question_phase_map m
  JOIN qset q ON q.question_id = m.question_id
)
UPDATE lca_result
   SET max_phase_1 = (SELECT max_p1 FROM mx),
       max_phase_2 = (SELECT max_p2 FROM mx),
       max_phase_3 = (SELECT max_p3 FROM mx),
       max_phase_4 = (SELECT max_p4 FROM mx),
       max_total   = (SELECT max_p1 + max_p2 + max_p3 + max_p4 FROM mx)
 WHERE run_id = :run_id
""")

def rebuild_loss_run(run_id: int):
    """
    End-to-end rebuild for a run:
    - Materialize lca_scorecard_run from latest responses + scorecard_v
    - Insert totals into lca_result (delete-then-insert, SQLite-safe)
    - Update max_* columns in lca_result
    Returns None. Use a SELECT afterwards if you want to display values.
    """
    with db.engine.begin() as conn:
        # 1) Materialize (idempotent via INSERT OR REPLACE + unique index)
        conn.execute(SQL_MATERIALIZE_SCORECARD_RUN, {"run_id": run_id})
        # 2) Totals -> lca_result
        conn.execute(SQL_DELETE_RESULT_FOR_RUN, {"run_id": run_id})
        conn.execute(SQL_INSERT_RESULT_FROM_SCORECARD_RUN, {"run_id": run_id})
        # 3) Max columns
        conn.execute(SQL_UPDATE_RESULT_MAX_COLUMNS, {"run_id": run_id})

"""
Put shared “LOSS” business logic here so both school_loss and admin.loss
can import it without importing each other.
- finalize_run_totals
- phase_items_for_score
- rebuild_scorecard_for_run
…copy the existing function *bodies* from wherever they live now.
"""



# EXAMPLES / STUBS — replace with your real implementations
def finalize_run_totals(run_id: int) -> None:
    """Compute & persist lca_result (and any aggregates) for a run."""
    # Paste your existing finalize SQL/logic here (from school_loss.routes).
    pass

def phase_items_for_score(run_id: int):
    """Return per-phase items for a run (whatever your admin routes expect)."""
    # Paste existing logic here.
    return []

def rebuild_scorecard_for_run(run_id: int) -> None:
    """Optional: recompute scorecard rows for a run."""
    # Paste existing logic here.
    pass
'''
def list_runs_from_lca_result(limit: int | None = None):
    sql = """
        SELECT run_id,
               COUNT(*)                      AS rows_count,
               MAX(COALESCE(created_at, id)) AS last_key
        FROM lca_result
        GROUP BY run_id
        ORDER BY last_key DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"

    result = db.session.execute(text(sql))
    rows = result.mappings().all()
    return [
        SimpleNamespace(
            run_id=r["run_id"],
            rows_count=r["rows_count"],
            last_at=r["last_key"],   # string/int; format later if you want
        )
        for r in rows
    ]

def _fmt_last(x):
    if isinstance(x, (datetime, date)):
        return x.strftime("%Y-%m-%d %H:%M")
    return str(x) if x is not None else None
# ❌ runs at import time — REMOVE this whole block
result = db.session.execute(text("""
    SELECT run_id,
           COUNT(*)                      AS rows_count,
           MAX(COALESCE(created_at, id)) AS last_key
    FROM lca_result
    GROUP BY run_id
    ORDER BY last_key DESC
"""))
rows = result.mappings().all()
runs = [
    SimpleNamespace(run_id=r["run_id"], rows_count=r["rows_count"], last_at=r["last_key"])
    for r in rows
]

# app/admin/loss/services.py

def list_runs_from_lca_result(limit: int | None = None):
    sql = """
        SELECT run_id,
               COUNT(*)                      AS rows_count,
               MAX(COALESCE(created_at, id)) AS last_key
        FROM lca_result
        GROUP BY run_id
        ORDER BY last_key DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"
    result = db.session.execute(text(sql))
    rows = result.mappings().all()
    return [
        SimpleNamespace(run_id=r["run_id"], rows_count=r["rows_count"], last_at=r["last_key"])
        for r in rows
    ]


def list_runs_from_lca_result(limit: int | None = None, format_ts: bool = False):
    sql = """
        SELECT run_id,
               COUNT(*)                      AS rows_count,
               MAX(COALESCE(created_at, id)) AS last_key
        FROM lca_result
        GROUP BY run_id
        ORDER BY last_key DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"

    result = db.session.execute(text(sql))
    rows = result.mappings().all()
    return [
        SimpleNamespace(
            run_id=r["run_id"],
            rows_count=r["rows_count"],
            last_at=_fmt_last(r["last_key"]) if format_ts else r["last_key"],
        )
        for r in rows
    ]

'''
from types import SimpleNamespace


def _fmt_last(x):
    if isinstance(x, (datetime, date)):
        return x.strftime("%Y-%m-%d %H:%M")
    return str(x) if x is not None else None

__all__ = ["list_runs_from_lca_result"]

def list_runs_from_lca_result(limit: int | None = None):
    """
    Return runs detected in lca_result, newest first.
    No DB work at import time. Safe to call from routes.
    """
    sql = """
        SELECT run_id,
               COUNT(*)                      AS rows_count,
               MAX(COALESCE(created_at, id)) AS last_key
        FROM lca_result
        GROUP BY run_id
        ORDER BY last_key DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"

    res = db.session.execute(text(sql))
    rows = res.mappings().all()
    return [
        SimpleNamespace(
            run_id=r["run_id"],
            rows_count=r["rows_count"],
            last_at=r["last_key"],  # keep raw; format in template or route if you like
        )
        for r in rows
    ]
