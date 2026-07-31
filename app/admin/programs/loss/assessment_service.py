# admin/loss/assessment_service.py
from sqlalchemy import text
from app.extensions import db

# ---------- Core helpers ----------

def record_answer(run_id: int, user_id: int, question_id: int, answer: str) -> None:
    """
    One-shot write for a single answer:
      1) upsert into lca_response
      2) upsert into lca_scorecard from lca_question_phase_map
      3) ensure a row in lca_result
      4) increment cumulative totals in lca_result
    """
    answer = (answer or "").strip().lower()

    with db.engine.begin() as conn:
        # 1) response (unique on (run_id, question_id))
        conn.execute(text("""
            INSERT INTO lca_response (user_id, run_id, question_id, answer)
            VALUES (:uid, :rid, :qid, :ans)
            ON CONFLICT(run_id, question_id) DO UPDATE SET
              user_id = excluded.user_id,
              answer  = excluded.answer
        """), {"uid": user_id, "rid": run_id, "qid": question_id, "ans": answer})

        # 2) scorecard row from map (unique on lca_scorecard(run_id, question_id))
        conn.execute(text("""
            INSERT INTO lca_scorecard
              (user_id, run_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
            SELECT :uid, :rid, :qid, :ans, m.phase_1, m.phase_2, m.phase_3, m.phase_4
            FROM lca_question_phase_map m
            WHERE m.question_id = :qid AND m.answer_type = :ans
            ON CONFLICT(run_id, question_id) DO UPDATE SET
              user_id     = excluded.user_id,
              answer_type = excluded.answer_type,
              phase_1     = excluded.phase_1,
              phase_2     = excluded.phase_2,
              phase_3     = excluded.phase_3,
              phase_4     = excluded.phase_4
        """), {"uid": user_id, "rid": run_id, "qid": question_id, "ans": answer})

        # 3) ensure result row exists (unique on lca_result.run_id)
        conn.execute(text("""
            INSERT OR IGNORE INTO lca_result (user_id, run_id, subject)
            VALUES (:uid, :rid, 'LOSS')
        """), {"uid": user_id, "rid": run_id})

        # 4) increment cumulative totals from the map for this exact (qid, ans)
        conn.execute(text("""
            UPDATE lca_result
               SET phase_1 = phase_1 + (
                         SELECT COALESCE(phase_1,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_2 = phase_2 + (
                         SELECT COALESCE(phase_2,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_3 = phase_3 + (
                         SELECT COALESCE(phase_3,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_4 = phase_4 + (
                         SELECT COALESCE(phase_4,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   total   = total + (
                         SELECT COALESCE(phase_1,0)+COALESCE(phase_2,0)+COALESCE(phase_3,0)+COALESCE(phase_4,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans)
             WHERE run_id = :rid
        """), {"rid": run_id, "qid": question_id, "ans": answer})


def finalize_run(run_id: int) -> None:
    """
    End-of-run: compute max_phase_* and max_total from the *actual questions*
    answered in this run and write them into lca_result.
    """
    with db.engine.begin() as conn:
        conn.execute(text("""
        WITH qset AS (
          SELECT DISTINCT question_id
          FROM lca_scorecard
          WHERE run_id = :rid
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
         WHERE run_id = :rid
        """), {"rid": run_id})


# ---------- Optional helpers ----------

def reset_run(run_id: int) -> None:
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM lca_response  WHERE run_id = :rid"), {"rid": run_id})
        conn.execute(text("DELETE FROM lca_scorecard WHERE run_id = :rid"), {"rid": run_id})
        conn.execute(text("DELETE FROM lca_result    WHERE run_id = :rid"), {"rid": run_id})
