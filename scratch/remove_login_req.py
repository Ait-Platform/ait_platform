import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Remove @login_required from provisioning routes and replace current_user.id
def fix_route(route_name, content):
    pattern = r'@sace_bp\.route\("' + route_name + r'".*?def \w+\(.*?\):'
    # Find the block, remove @login_required
    def replacer(match):
        s = match.group(0)
        return s.replace('@login_required\n', '')
    content = re.sub(pattern, replacer, content, flags=re.DOTALL)
    return content

routes = fix_route('/sace/provisioning', routes)
routes = fix_route('/sace/provisioning/pledge', routes)
routes = fix_route('/sace/provisioning/add_auditor', routes)
routes = fix_route('/sace/provisioning/edit_auditor/<int:auditor_id>', routes)

# Replace current_user.id with (current_user.id if current_user.is_authenticated else 1) in provisioning map
map_old = '''    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()'''

map_new = '''    # Use admin user 1 as a placeholder for the unauthenticated SACE admin guest
    sace_user_id = current_user.id if current_user.is_authenticated else 1

    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()'''

routes = routes.replace(map_old, map_new)


pledge_old = '''    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.flush()
        from app.models.core import CoreAuditEvent
        db.session.add(CoreAuditEvent(
            user_id=current_user.id,'''

pledge_new = '''    sace_user_id = current_user.id if current_user.is_authenticated else 1
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.flush()
        from app.models.core import CoreAuditEvent
        db.session.add(CoreAuditEvent(
            user_id=sace_user_id,'''
            
routes = routes.replace(pledge_old, pledge_new)


add_old = '''        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="auditor_provisioned",
            response_data=json.dumps(data)
        )
        db.session.add(interaction)
        db.session.flush()
        from app.models.core import CoreAuditEvent
        db.session.add(CoreAuditEvent(
            user_id=current_user.id,'''
            
add_new = '''        sace_user_id = current_user.id if current_user.is_authenticated else 1
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug="auditor_provisioned",
            response_data=json.dumps(data)
        )
        db.session.add(interaction)
        db.session.flush()
        from app.models.core import CoreAuditEvent
        db.session.add(CoreAuditEvent(
            user_id=sace_user_id,'''
            
routes = routes.replace(add_old, add_new)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
