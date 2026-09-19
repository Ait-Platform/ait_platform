from wsgi import app
from app.models.uip_governance import UipCommitteeMember
with app.app_context():
    for m in UipCommitteeMember.query.all():
        print(f"Name: {m.name}, Position: {m.position}")
