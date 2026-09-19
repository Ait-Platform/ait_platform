with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """@uip_bp.route("/<org_slug>/verify/ratepayer", methods=["GET", "POST"])
@login_required
def verify_ratepayer(org_slug):
    # Check if they already have authority
    role = _require_role("owner", "resident", abort_on_fail=False)
    if role:
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug, as_ratepayer=1))
    
    from app.models.core import CoreInteraction
    from app import db
    from flask_login import current_user
    
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="ratepayer_claim", title="Ratepayer Claim",
            description=f"User {current_user.email} claims to be a ratepayer.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
    
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="ratepayer"))"""

new_logic = """@uip_bp.route("/<org_slug>/verify/ratepayer", methods=["GET", "POST"])
@login_required
def verify_ratepayer(org_slug):
    # Check if they already have authority
    role = _require_role("owner", "resident", abort_on_fail=False)
    if role:
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug, as_ratepayer=1))
    
    from app.models.core import CoreInteraction, CoreRoleAssignment, CoreRole, CoreOrganizationMember
    from app.models.uip import UipMemberProfile
    from sqlalchemy import func
    from flask import flash
    
    # 1. Instant Vault Lookup
    vault_match = UipMemberProfile.query.filter(
        UipMemberProfile.organization_id == g.organization.id,
        UipMemberProfile.is_active == True,
        func.lower(UipMemberProfile.email) == func.lower(current_user.email)
    ).first()
    
    if vault_match:
        # User is securely verified against the internal register
        membership = CoreOrganizationMember.query.filter_by(organization_id=g.organization.id, user_id=current_user.id).first()
        if not membership:
            membership = CoreOrganizationMember(organization_id=g.organization.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
            db.session.flush()
            
        role_obj = CoreRole.query.filter_by(slug="resident").first()
        if role_obj:
            existing = CoreRoleAssignment.query.filter_by(organization_id=g.organization.id, user_id=current_user.id, role_id=role_obj.id).first()
            if not existing:
                db.session.add(CoreRoleAssignment(organization_id=g.organization.id, user_id=current_user.id, role_id=role_obj.id))
                
        # Register a verified claim footprint
        claim = CoreInteraction.query.filter_by(
            organization_id=g.organization.id, creator_id=current_user.id, interaction_type="ratepayer_claim"
        ).first()
        if not claim:
            claim = CoreInteraction(
                organization_id=g.organization.id, creator_id=current_user.id,
                interaction_type="ratepayer_claim", title="Ratepayer Claim - Vault Auto-Verify",
                description=f"User {current_user.email} verified instantly via Municipal Vault match.", status="VERIFIED"
            )
            db.session.add(claim)
        else:
            claim.status = "VERIFIED"
            
        db.session.commit()
        flash("Welcome! Your account was instantly verified against the Municipal Vault.", "success")
        return redirect(url_for("uip_bp.dashboard", org_slug=org_slug, as_ratepayer=1))
    
    # 2. If NO vault match, they must proceed via manual intake
    claim = CoreInteraction.query.filter_by(
        organization_id=g.organization.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="OPEN"
    ).first()
    if not claim:
        claim = CoreInteraction(
            organization_id=g.organization.id, creator_id=current_user.id,
            interaction_type="ratepayer_claim", title="Ratepayer Claim",
            description=f"User {current_user.email} claims to be a ratepayer.", status="OPEN"
        )
        db.session.add(claim)
        db.session.commit()
    
    return redirect(url_for("uip_bp.my_access", org_slug=org_slug, claim="ratepayer"))"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated ratepayer verification logic")
