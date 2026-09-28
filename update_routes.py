import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update secretary_intake GET route to fetch processed claims
get_route_search = '''    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(UipResolution.decision_date.desc().nullslast(), UipResolution.id.desc()).all()

    return render_template('''
get_route_replace = '''    adopted_resolutions = UipResolution.query.filter_by(
        organization_id=org.id, 
        status='ADOPTED'
    ).order_by(UipResolution.decision_date.desc().nullslast(), UipResolution.id.desc()).all()

    processed_claims = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status.in_(["VERIFIED", "PENDING_RESOLUTION"]),
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).order_by(CoreInteraction.id.desc()).limit(15).all()
    
    enriched_processed = []
    for pc in processed_claims:
        creator = User.query.get(pc.creator_id)
        enriched_processed.append({
            "id": pc.id,
            "type": pc.interaction_type,
            "title": pc.title,
            "status": pc.status,
            "user_name": creator.name or "User",
            "user_email": creator.email,
        })

    return render_template('''
content = content.replace(get_route_search, get_route_replace)

# 2. Add processed_claims to render_template
render_search = '''        existing_mo=existing_mo,
        mo_conflict=mo_conflict,
        adopted_resolutions=adopted_resolutions
    )'''
render_replace = '''        existing_mo=existing_mo,
        mo_conflict=mo_conflict,
        adopted_resolutions=adopted_resolutions,
        processed_claims=enriched_processed
    )'''
content = content.replace(render_search, render_replace)

# 3. Add undo_claim route
undo_route = '''
@uip_bp.route("/<org_slug>/undo-claim/<int:claim_id>", methods=["POST"])
@login_required
def undo_claim(org_slug, claim_id):
    org = g.organization
    _require_secretary()
    from app.models.core import CoreInteraction, CoreRoleAssignment, CoreRole
    from app.models.uip_governance import UipCommitteeMember
    
    claim = CoreInteraction.query.filter_by(organization_id=org.id, id=claim_id).first_or_404()
    
    if claim.status == "VERIFIED":
        # Revoke roles and committee membership
        UipCommitteeMember.query.filter_by(organization_id=org.id, user_id=claim.creator_id, status="CURRENT").delete()
        CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=claim.creator_id).delete()
        claim.resolution_id = None
        flash("Verification revoked. Applicant returned to waiting room.", "info")
    elif claim.status == "PENDING_RESOLUTION":
        flash("Applicant removed from proposed resolution track and returned to waiting room.", "info")
        
    claim.status = "OPEN"
    db.session.commit()
    
    return redirect(url_for("uip_bp.secretary_intake", org_slug=org.slug))
'''
content += undo_route

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated secretary_routes.py")
