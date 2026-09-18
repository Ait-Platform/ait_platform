from app import create_app
from app import db
from app.models.uip import UipResolution

app = create_app()
with app.app_context():
    # Only insert if they don't exist
    existing = UipResolution.query.filter_by(organization_id=1).count()
    if existing == 0:
        foundational_resolutions = [
            {"title": "Founding Declaration", "desc": "Formal establishment of the Precinct and adoption of the constitution."},
            {"title": "Access Bundle", "desc": "Batched approval of initial verified members and ratepayers."},
            {"title": "Manager Designation", "desc": "Delegation of operational authority to precinct staff and supervisors."},
            {"title": "Token Wallet Authorization", "desc": "Adoption of the AIT platform and authorization of token expenditure."}
        ]
        for res_data in foundational_resolutions:
            new_res = UipResolution(
                organization_id=1,
                title=res_data["title"],
                description=res_data["desc"],
                status="PROPOSED",
                voting_scope="EXCO",
                quorum_target=50
            )
            db.session.add(new_res)
        db.session.commit()
        print("Generated 4 foundational resolutions for org 1")
    else:
        print("Resolutions already exist for org 1")
