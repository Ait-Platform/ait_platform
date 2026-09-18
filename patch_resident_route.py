import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_resident_route = """    if role_slug in {"resident", "owner"}:
        interactions = CoreInteraction.query.filter_by(
            organization_id=org.id, creator_id=current_user.id
        ).all()
        return render_template("program_uip/dashboards/resident.html", org=org, interactions=interactions)"""

new_resident_route = """    if role_slug in {"resident", "owner"}:
        interactions = CoreInteraction.query.filter_by(
            organization_id=org.id, creator_id=current_user.id
        ).all()
        
        from app.models.uip import UipResolution
        public_votes = UipResolution.query.filter_by(
            organization_id=org.id,
            status="PROPOSED",
            voting_scope="PUBLIC"
        ).order_by(UipResolution.created_at.desc()).all()
        
        return render_template("program_uip/dashboards/resident.html", org=org, interactions=interactions, public_votes=public_votes)"""

text = text.replace(old_resident_route, new_resident_route)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resident dashboard route")
