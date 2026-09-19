with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        elif not membership.is_active:
            membership.is_active = True
        db.session.commit()
        pos = appointment.position.lower()"""

new_logic = """        elif not membership.is_active:
            membership.is_active = True
        db.session.commit()
        
        # --- AUTO-FIX FOR PROTOTYPE RECORDS ---
        pos_raw = appointment.position.strip().lower() if appointment.position else ""
        if pos_raw in ["committee", "unassigned", "committee member", ""]:
            from app.models.core import CoreInteraction
            claim = CoreInteraction.query.filter_by(creator_id=current_user.id, interaction_type="committee_claim").first()
            if claim and ":" in claim.title:
                new_pos = claim.title.split(": ")[-1]
                appointment.position = new_pos
                db.session.commit()
                
        # Ensure committee_member role is set
        from app.models.core import CoreRoleAssignment, CoreRole
        role_obj = CoreRole.query.filter_by(slug="committee_member").first()
        if role_obj:
            if not CoreRoleAssignment.query.filter_by(organization_id=org.id, user_id=current_user.id, role_id=role_obj.id).first():
                db.session.add(CoreRoleAssignment(organization_id=org.id, user_id=current_user.id, role_id=role_obj.id))
                db.session.commit()
        # --------------------------------------

        pos = appointment.position.lower()"""

if old_logic in text:
    text = text.replace(old_logic, new_logic)
    print("Patched router_page in routes.py")
else:
    print("Could not find router_page logic")

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
