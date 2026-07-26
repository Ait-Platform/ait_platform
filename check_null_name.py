
from app import create_app, db
from app.models.culturalfire import CfiPageantSegment, CfiTalentSubmission
app = create_app()
with app.app_context():
    segments = CfiPageantSegment.query.all()
    for s in segments:
        print(f'Segment {s.id}: name={s.name}')
    subs = CfiTalentSubmission.query.all()
    print(f'Found {len(subs)} submissions.')

