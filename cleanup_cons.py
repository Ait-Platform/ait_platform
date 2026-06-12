from app import create_app
from app.models.billing import db, BilConsumption

app = create_app()
with app.app_context():
    # Find duplicate consumptions for the same meter and month
    consumptions = BilConsumption.query.all()
    seen = set()
    deleted = 0
    for c in consumptions:
        key = (c.meter_id, c.month)
        if key in seen:
            db.session.delete(c)
            deleted += 1
        else:
            seen.add(key)
    if deleted > 0:
        db.session.commit()
    print(f"Deleted {deleted} duplicate consumptions")
