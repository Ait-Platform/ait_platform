with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        if current_appointment:
            pos = current_appointment.position.strip().lower() if current_appointment.position else ""
            if pos in ["chairman", "vice-chairperson", "vice chairman", "chair", "chairperson", "vice chair"]:"""

new_logic = """        if current_appointment:
            pos = current_appointment.position.strip().lower() if current_appointment.position else ""
            
            # --- AUTO-FIX FOR PROTOTYPE RECORDS ---
            if pos in ["committee", "unassigned", "committee member", ""]:
                claim = CoreInteraction.query.filter_by(creator_id=current_user.id, interaction_type="committee_claim").first()
                if claim and ":" in claim.title:
                    new_pos = claim.title.split(": ")[-1]
                    current_appointment.position = new_pos
                    db.session.commit()
                    pos = new_pos.strip().lower()
            
            # Ensure they have committee_member role for the sidebar financial buttons
            from app.models.core import CoreRoleAssignment, CoreRole
            role_obj = CoreRole.query.filter_by(slug="committee_member").first()
            if role_obj:
                existing_role = CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=current_user.id, role_id=role_obj.id).first()
                if not existing_role:
                    db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=current_user.id, role_id=role_obj.id))
                    db.session.commit()
            # --------------------------------------

            if pos in ["chairman", "vice-chairperson", "vice chairman", "chair", "chairperson", "vice chair"]:"""

if old_logic in text:
    text = text.replace(old_logic, new_logic)
    print("Patched routes.py with auto-fix")
else:
    print("Could not find logic in routes.py")

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
