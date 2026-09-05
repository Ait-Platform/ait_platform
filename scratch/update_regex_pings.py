import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# 1. Pledge
routes = re.sub(
    r'(activity_slug="admin_patent_pledge".*?db\.session\.add\(interaction\))',
    r'\1\n        db.session.flush()\n        from app.models.core import CoreAuditEvent\n        db.session.add(CoreAuditEvent(user_id=current_user.id, action="SACE_PLEDGE_ACCEPTED", entity_type="SaceWorkshopInteraction", entity_id=interaction.id, details="SACE Administrator accepted the Intellectual Property Pledge."))',
    routes, flags=re.DOTALL
)

# 2. Auditor
routes = re.sub(
    r'(activity_slug="auditor_provisioned".*?db\.session\.add\(interaction\))',
    r'\1\n        db.session.flush()\n        from app.models.core import CoreAuditEvent\n        db.session.add(CoreAuditEvent(user_id=current_user.id, action="SACE_AUDITOR_PROVISIONED", entity_type="SaceWorkshopInteraction", entity_id=interaction.id, details=f"SACE Admin provisioned auditor: {first_name} {last_name} ({email})"))',
    routes, flags=re.DOTALL
)

# 3. Post Test
routes = re.sub(
    r'(activity_slug=\'workshop_post_test\'.*?db\.session\.add\(interaction\))',
    r'\1\n      db.session.flush()\n      from app.models.core import CoreAuditEvent\n      db.session.add(CoreAuditEvent(user_id=current_user.id, action="SACE_EVALUATION_COMPLETED", entity_type="SaceWorkshopInteraction", entity_id=interaction.id, details=f"SACE Auditor submitted final evaluation. Score: {score}%"))',
    routes, flags=re.DOTALL
)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
