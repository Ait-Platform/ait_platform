import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Replace any remaining current_user.id with sace_user_id in those 3 routes
# Wait, let's just make it a global replace for the specific lines
def robust_replace():
    global routes
    routes = re.sub(
        r'pledge = SaceWorkshopInteraction\.query\.filter_by\(user_id=current_user\.id, activity_slug="admin_patent_pledge"\)\.first\(\)',
        r'sace_user_id = current_user.id if current_user.is_authenticated else 1\n    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()',
        routes
    )
    
    routes = re.sub(
        r'interaction = SaceWorkshopInteraction\(\s*user_id=current_user\.id,\s*activity_slug="admin_patent_pledge"',
        r'interaction = SaceWorkshopInteraction(\n            user_id=sace_user_id,\n            activity_slug="admin_patent_pledge"',
        routes
    )
    
    routes = re.sub(
        r'db\.session\.add\(CoreAuditEvent\(\s*user_id=current_user\.id,\s*action="SACE_PLEDGE_ACCEPTED"',
        r'db.session.add(CoreAuditEvent(\n            user_id=sace_user_id,\n            action="SACE_PLEDGE_ACCEPTED"',
        routes
    )

    routes = re.sub(
        r'interaction = SaceWorkshopInteraction\(\s*user_id=current_user\.id,\s*activity_slug="auditor_provisioned"',
        r'sace_user_id = current_user.id if current_user.is_authenticated else 1\n        interaction = SaceWorkshopInteraction(\n            user_id=sace_user_id,\n            activity_slug="auditor_provisioned"',
        routes
    )
    
    routes = re.sub(
        r'db\.session\.add\(CoreAuditEvent\(\s*user_id=current_user\.id,\s*action="SACE_AUDITOR_PROVISIONED"',
        r'db.session.add(CoreAuditEvent(\n            user_id=sace_user_id,\n            action="SACE_AUDITOR_PROVISIONED"',
        routes
    )

robust_replace()

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
