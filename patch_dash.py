with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """    if CoreInteraction.query.filter_by(organization_id=org.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="VERIFIED").first():
        resident_role = CoreRole.query.filter_by(slug="owner").first() or CoreRole.query.filter_by(slug="resident").first()"""

new_logic = """    if CoreInteraction.query.filter_by(organization_id=org.id, creator_id=current_user.id, interaction_type="ratepayer_claim", status="VERIFIED").first():
        membership = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=current_user.id).first()
        if membership and not membership.is_active:
            membership.is_active = True
            db.session.commit()
        resident_role = CoreRole.query.filter_by(slug="owner").first() or CoreRole.query.filter_by(slug="resident").first()"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched dashboard auto-fix")
