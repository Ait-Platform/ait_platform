from app import create_app
from app.extensions import db
from app.models.mechanic import MechJobCard
import sys

app = create_app()
with app.app_context():
    job = MechJobCard.query.first()
    print(f"Job Card: {job.job_number}, Client ID: {job.vehicle.client.id}, Client user_id: {job.vehicle.client.user_id}")
