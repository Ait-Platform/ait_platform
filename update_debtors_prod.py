import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
app.config['SQLALCHEMY_DATABASE_URI'] = PG_URL

with app.app_context():
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if subject:
        subject.allow_country_pricing = 1
        db.session.commit()
        print('Updated debtors subject to allow country pricing on production.')
