import os
os.environ["DATABASE_URL"] = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

from app import create_app
from app.extensions import db
from app.subject_loss.services import compute_loss_results
from sqlalchemy import text

app = create_app()
with app.app_context():
    print("Finding runs for loss1@gmail.com on Render:")
    runs = db.session.execute(text("SELECT id FROM lca_run WHERE user_id = (SELECT id FROM \"user\" WHERE email='loss1@gmail.com') ORDER BY id DESC")).fetchall()
    print("Runs:", runs)
    
    for r in runs:
        run_id = r[0]
        print(f"Before compute {run_id}:")
        print(db.session.execute(text(f"SELECT * FROM lca_result WHERE run_id={run_id}")).fetchall())
        compute_loss_results(run_id)
        print(f"After compute {run_id}:")
        print(db.session.execute(text(f"SELECT * FROM lca_result WHERE run_id={run_id}")).fetchall())
