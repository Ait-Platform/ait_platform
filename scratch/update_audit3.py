import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

pattern_pledge = r'(flash\("Intellectual Property pledge accepted\. Provisioning unlocked\.", "success"\))'
replacement_pledge = r'''from app.models.core import CoreAuditEvent
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        audit = CoreAuditEvent(
            user_id=sace_user_id,
            action="PLEDGE_ACCEPTED",
            entity_type="SACE_PLEDGE",
            details="Admin accepted IP pledge",
            ip_address=ip_addr
        )
        db.session.add(audit)
        db.session.commit()
        \1'''

text = re.sub(pattern_pledge, replacement_pledge, text)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
