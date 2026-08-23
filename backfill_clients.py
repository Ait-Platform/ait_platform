import os
from app import create_app
from app.extensions import db
from app.models.mechanic import MechClient, MechJobCard, MechVehicle, MechShop

app = create_app()
with app.app_context():
    # Fix any MechClient missing user_id
    # We can infer it from the first active shop, since this is mostly a single-tenant or small platform right now
    active_shop = MechShop.query.first()
    if active_shop:
        clients = MechClient.query.filter_by(user_id=None).all()
        for c in clients:
            c.user_id = active_shop.user_id
        db.session.commit()
        print(f"Updated {len(clients)} clients to user_id {active_shop.user_id}")
