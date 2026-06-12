from app import create_app
from app.models.billing import BilProperty

app = create_app()
with app.app_context():
    p3 = BilProperty.query.get(3)
    p8 = BilProperty.query.get(8)
    print("Prop 3:", p3.name if p3 else 'Not found')
    print("Prop 8:", p8.name if p8 else 'Not found')
