# app/jobs/loss_archive.py
from sqlalchemy import text
from app.extensions import db  # adjust if your app factory exposes db elsewhere

def ensure_archive_tables():
    with db.engine.begin() as conn:
        # create empty archive tables with same columns
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS lca_run_archive           AS SELECT * FROM lca_run WHERE 0;
            CREATE TABLE IF NOT EXISTS lca_result_archive        AS SELECT * FROM lca_result WHERE 0;
            CREATE TABLE IF NOT EXISTS lca_response_archive      AS SELECT * FROM lca_response WHERE 0;
        """)
        # useful hot indexes (no-op if they exist)
        conn.exec_driver_sql("""
            CREATE INDEX IF NOT EXISTS ix_lca_run_user_started ON lca_run (user_id, started_at DESC);
            CREATE INDEX IF NOT EXISTS ix_lca_response_run     ON lca_response (run_id);
            CREATE INDEX IF NOT EXISTS ix_lca_result_run       ON lca_result (run_id);
        """)

def archive_finished_runs(older_than_days=30, limit=500):
    ensure_archive_tables()
    with db.engine.begin() as conn:
        # pick candidates
        rows = conn.execute(text("""
            SELECT id FROM lca_run
             WHERE status='finished'
               AND started_at <= datetime('now', :offset)
            ORDER BY datetime(started_at) ASC
            LIMIT :lim
        """), {"offset": f"-{older_than_days} days", "lim": limit}).fetchall()

        run_ids = [r[0] for r in rows]
        if not run_ids:
            return 0

        id_list = ",".join(str(x) for x in run_ids)

        # move children first, then parent
        conn.exec_driver_sql(f"""
            INSERT INTO lca_response_archive SELECT * FROM lca_response WHERE run_id IN ({id_list});
            DELETE FROM lca_response WHERE run_id IN ({id_list});
        """)
        conn.exec_driver_sql(f"""
            INSERT INTO lca_result_archive SELECT * FROM lca_result WHERE run_id IN ({id_list});
            DELETE FROM lca_result WHERE run_id IN ({id_list});
        """)
        conn.exec_driver_sql(f"""
            INSERT INTO lca_run_archive SELECT * FROM lca_run WHERE id IN ({id_list});
            DELETE FROM lca_run WHERE id IN ({id_list});
        """)

        return len(run_ids)
