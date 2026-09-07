import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_map_flush = '''    # If they are logged in and have a session pledge, save it to DB now
    if current_user.is_authenticated and session.get('sace_admin_pledged'):
        existing = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not existing:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            db.session.commit()
        session.pop('sace_admin_pledged', None)'''

new_map_flush = '''    # If they are logged in and have a session pledge, save it to DB now
    if current_user.is_authenticated and session.get('sace_admin_pledged'):
        existing = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not existing:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            
            from app.models.core import CoreAuditEvent
            ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
            audit = CoreAuditEvent(
                user_id=current_user.id,
                action="PLEDGE_ACCEPTED",
                entity_type="SACE_PLEDGE",
                details="Admin accepted IP pledge",
                ip_address=ip_addr
            )
            db.session.add(audit)
            db.session.commit()
        session.pop('sace_admin_pledged', None)'''

text = text.replace(old_map_flush, new_map_flush)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
