import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_patent = '''        db.session.add(interaction)
        db.session.commit()
        flash("Intellectual Property acknowledged.", "success")'''

new_patent = '''        db.session.add(interaction)
        db.session.commit()
        
        from app.models.core import CoreAuditEvent
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        audit = CoreAuditEvent(
            user_id=current_user.id,
            action="PLEDGE_ACCEPTED",
            entity_type="SACE_PLEDGE",
            details="Evaluator accepted IP pledge on map",
            ip_address=ip_addr
        )
        db.session.add(audit)
        db.session.commit()
        
        flash("Intellectual Property acknowledged.", "success")'''

text = text.replace(old_patent, new_patent)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
