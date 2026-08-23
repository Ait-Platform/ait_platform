import os
from app import create_app
from app.extensions import db
from app.models.mechanic import MechClient, MechJobCard, MechVehicle, MechShop

app = create_app()
with app.app_context():
    active_shop = MechShop.query.first()
    if active_shop:
        clients = MechClient.query.all()
        for c in clients:
            c.user_id = active_shop.user_id
        db.session.commit()
        print(f"Updated all {len(clients)} clients to user_id {active_shop.user_id}")
