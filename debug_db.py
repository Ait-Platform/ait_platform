from app import create_app
from app.extensions import db
from app.models.uip import UipResolution, UipResolutionVote
from app.models.uip_governance import UipCommitteeMember

app = create_app()
with app.app_context():
    org_id = 1
    resolutions = UipResolution.query.filter_by(organization_id=org_id).all()
    for res in resolutions:
        votes = UipResolutionVote.query.filter_by(resolution_id=res.id).count()
        print(f"Res {res.id} - Ref: {res.reference} - Status: {res.status} - Votes: {votes}")
        
    members = UipCommitteeMember.query.filter_by(organization_id=org_id, status="CURRENT").count()
    print(f"Total CURRENT EXCO members: {members}")
