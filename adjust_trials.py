from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    # Update all trials that are far in the future to expire in 2 minutes
    db.session.execute(text("UPDATE user_enrollment SET trial_end = CURRENT_TIMESTAMP + INTERVAL '2 minutes' WHERE status = 'active' AND trial_end IS NOT NULL"))
    db.session.execute(text("UPDATE user_enrollment SET trial_end = CURRENT_TIMESTAMP + INTERVAL '2 minutes' WHERE status = 'trial' AND trial_end IS NOT NULL"))
    db.session.commit()
    print("Done adjusting trials")
