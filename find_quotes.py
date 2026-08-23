import os
from app import create_app
from app.extensions import db
from app.models.mechanic import MechClient, MechJobCard, MechVehicle

app = create_app()
with app.app_context():
    jobs = MechJobCard.query.all()
    for j in jobs:
        print(f"Job: {j.job_number}, Client ID: {j.vehicle.client_id if j.vehicle else 'No Vehicle'}, Client Name: {j.vehicle.client.name if j.vehicle and j.vehicle.client else 'Unknown'}, User ID: {j.vehicle.client.user_id if j.vehicle and j.vehicle.client else 'Unknown'}")
