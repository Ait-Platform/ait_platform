with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """def dashboard(org_slug):
    org = g.organization
    role_slug = _require_role("owner", "resident", abort_on_fail=False) if request.args.get("as_ratepayer") else _require_role("manager", "receptionist", "committee_member", "owner", "resident", "provider", "municipal_officer", abort_on_fail=False)"""

new_logic = """def dashboard(org_slug):
    org = g.organization
    
    # --- AUTO-FIX FOR RATEPAYER ROLES ---
    from app.models.core import CoreInteraction, CoreRoleAssignment, CoreRole
    from app.extensions import db
    if CoreInteraction.query.filter_by(organization_id=org.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="VERIFIED").first():
        resident_role = CoreRole.query.filter_by(slug="resident").first()
        if resident_role:
            if not CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=current_user.id, role_id=resident_role.id).first():
                db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=current_user.id, role_id=resident_role.id))
                db.session.commit()
    # ------------------------------------
    
    role_slug = _require_role("owner", "resident", abort_on_fail=False) if request.args.get("as_ratepayer") else _require_role("manager", "receptionist", "committee_member", "owner", "resident", "provider", "municipal_officer", abort_on_fail=False)"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched dashboard auto-fix for ratepayers")
