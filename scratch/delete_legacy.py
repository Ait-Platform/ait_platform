from app import create_app
from app.extensions import db
from app.models.sace import SaceWorkshopInteraction
import json

app = create_app()
with app.app_context():
    interactions = SaceWorkshopInteraction.query.filter_by(activity_slug='auditor_provisioned').all()
    count = 0
    for ix in interactions:
        try:
            data = json.loads(ix.response_data)
            if 'code' not in data:
                db.session.delete(ix)
                count += 1
        except:
            db.session.delete(ix)
            count += 1
    db.session.commit()
    print(f"Legacy records deleted: {count}")
