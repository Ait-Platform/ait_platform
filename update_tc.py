from app import create_app, db
from app.models.mechanic import MechShop

app = create_app()
with app.app_context():
    shops = MechShop.query.all()
    default_tc = "1. THANK YOU FOR YOUR SUPPORT!\n2. All work is completed to a high standard using genuine parts.\n3. Parts remain the property of the business until paid in full.\n4. Vehicles are stored and driven at owner's risk."
    for s in shops:
        if not s.terms_and_conditions or len(s.terms_and_conditions.strip()) < 10:
            s.terms_and_conditions = default_tc
    db.session.commit()
    print("Updated T&Cs for shops.")
