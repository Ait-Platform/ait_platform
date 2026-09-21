with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

helper = """
def _get_next_unvoted_resolution(org_id, user_id):
    from app.models.uip import UipResolution, UipResolutionVote
    from app.extensions import db
    
    # Subquery: get IDs of all resolutions this user has voted on
    voted_subquery = db.session.query(UipResolutionVote.resolution_id).filter(
        UipResolutionVote.user_id == user_id
    ).subquery()
    
    # Query: find first PROPOSED resolution NOT in the subquery
    next_res = UipResolution.query.filter(
        UipResolution.organization_id == org_id,
        UipResolution.status == "PROPOSED",
        ~UipResolution.id.in_(voted_subquery)
    ).order_by(UipResolution.id.asc()).first()
    
    return next_res
"""

if "def _get_next_unvoted_resolution" not in text:
    text = helper + "\n" + text

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Helper added")
