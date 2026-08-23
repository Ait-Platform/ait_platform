from app import create_app
from app.extensions import db
from app.models.debtors import Debtor

app = create_app()
with app.app_context():
    debtors = Debtor.query.all()
    for d in debtors:
        print(d.id, d.name, d.slug_reference)
