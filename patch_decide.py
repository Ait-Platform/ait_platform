import re

with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_decide = """    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED"]:
        abort(400)
        
    res.status = decision"""

new_decide = """    decision = request.form.get("decision")
    if decision not in ["ADOPTED", "REJECTED"]:
        abort(400)
        
    # Enforce Quorum for Adoption
    if decision == "ADOPTED":
        votes = res.votes.all() if hasattr(res, 'votes') else []
        quorum_target = getattr(res, 'quorum_target', 50)
        scope = getattr(res, 'voting_scope', 'EXCO')
        
        total_eligible = 0
        if scope == 'EXCO':
            total_eligible = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").count()
        else:
            from app.models.core import CoreOrganizationMember
            total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
            
        if total_eligible == 0:
            total_eligible = 1
            
        current_quorum_pct = int((len(votes) / total_eligible) * 100)
        
        if current_quorum_pct < quorum_target:
            flash(f"Cannot adopt: Quorum not met ({current_quorum_pct}% of {quorum_target}% required).", "danger")
            return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
            
    res.status = decision"""

text = text.replace(old_decide, new_decide)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated decide logic")
