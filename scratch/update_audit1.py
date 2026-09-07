import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Fix document_action
old_doc_log = '''        existing = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug=slug).first()
    if not existing:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug=slug,
            response_data=json.dumps({"action": action, "doc_id": doc_id})
        )
        db.session.add(interaction)
        db.session.commit()'''

new_doc_log = '''        existing = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug=slug).first()
    if not existing:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug=slug,
            response_data=json.dumps({"action": action, "doc_id": doc_id})
        )
        db.session.add(interaction)
        db.session.commit()
        
    # Also log to Platform Audit Report
    from app.models.core import CoreAuditEvent
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    doc_titles = {
        "1": "SACE Provider Application Form",
        "2": "SACE Activity Application Form",
        "3": "SACE Endorsement Guidelines",
        "4": "Facilitator CVs"
    }
    title = doc_titles.get(doc_id, f"Document {doc_id}")
    audit = CoreAuditEvent(
        user_id=sace_user_id,
        action="DOCUMENT_ACCESSED" if action == "view" else "DOCUMENT_EMAILED",
        entity_type="SACE_DOCUMENT",
        details=f"Admin {action}ed '{title}'",
        ip_address=ip_addr
    )
    db.session.add(audit)
    db.session.commit()'''

text = text.replace(old_doc_log, new_doc_log)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
