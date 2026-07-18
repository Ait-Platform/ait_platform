from app import create_app
from app.extensions import db
from sqlalchemy import text
app = create_app()
with app.app_context():
    db.session.execute(text("INSERT INTO system_settings (key, value) VALUES ('practice_enquiry_cents', '500') ON CONFLICT DO NOTHING"))
    db.session.commit()
    print('Practice pricing settings added')
