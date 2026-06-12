from app import create_app
from app.extensions import db
from app.models.billing import BilTariff
from datetime import date

app = create_app()
with app.app_context():
    t = BilTariff.query.filter_by(utility_type='electricity').first()
    if not t:
        t = BilTariff(utility_type='electricity', rate=3.2795, code='ELEC01', description='Standard Electricity Tariff', effective_date=date(2026, 1, 1))
        db.session.add(t)
    else:
        t.rate = 3.2795
    db.session.commit()
    print("Tariff saved.")
