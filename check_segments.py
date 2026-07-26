from app import create_app
from app.models.culturalfire import CfiPageantSegment

app = create_app()

with app.app_context():
    segments = CfiPageantSegment.query.all()
    for s in segments:
        print(f"Segment ID: {s.id}, Name: '{s.name}'")
