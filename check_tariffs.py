from app import create_app
from app.extensions import db
from app.models.billing import BilTariff

app = create_app()
with app.app_context():
    tariffs = BilTariff.query.all()
    print([(t.utility_type, t.rate) for t in tariffs])
