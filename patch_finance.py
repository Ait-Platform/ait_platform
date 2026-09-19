with open("app/program_uip/finance_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """def finance_overview(org_slug):
    org, actor, admin = context()
    overview = f.overview(org, actor, request.args.get("year"))"""

new_logic = """def finance_overview(org_slug):
    org, actor, admin = context()
    
    # --- AUTO-FIX FOR PROTOTYPE RECORDS ---
    from app.models.core import CoreRoleAssignment, CoreRole
    role_obj = CoreRole.query.filter_by(slug="committee_member").first()
    if role_obj:
        existing_role = CoreRoleAssignment.query.filter_by(organization_id=org, user_id=actor, role_id=role_obj.id).first()
        if not existing_role:
            from app.extensions import db
            db.session.add(CoreRoleAssignment(organization_id=org, user_id=actor, role_id=role_obj.id))
            db.session.commit()
    # --------------------------------------
    
    overview = f.overview(org, actor, request.args.get("year"))"""

if old_logic in text:
    text = text.replace(old_logic, new_logic)
    print("Patched finance_routes.py with auto-fix")
else:
    print("Could not find logic in finance_routes.py")

with open("app/program_uip/finance_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
