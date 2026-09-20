with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        if not membership:
            membership = CoreOrganizationMember(organization_id=g.organization.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
            db.session.flush()"""

new_logic = """        if not membership:
            membership = CoreOrganizationMember(organization_id=g.organization.id, user_id=current_user.id, is_active=True)
            db.session.add(membership)
            db.session.flush()
        else:
            membership.is_active = True"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched verify_ratepayer is_active logic")
