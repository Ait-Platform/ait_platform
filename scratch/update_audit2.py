import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace in document_action
# Look for: db.session.commit() right before doc_file_map = {
pattern_doc = r'(db\.session\.commit\(\)\s+)(doc_file_map = \{)'
replacement_doc = r'''\1
    # Also log to Platform Audit Report
    from app.models.core import CoreAuditEvent
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    doc_titles = {
        "1": "SACE Provider Application Form",
        "2": "SACE Activity Application Form",
        "3": "SACE Endorsement Guidelines",
        "4": "Facilitator CVs"
    }
    title = doc_titles.get(str(doc_id), f"Document {doc_id}")
    audit = CoreAuditEvent(
        user_id=sace_user_id,
        action="DOCUMENT_ACCESSED" if action == "view" else "DOCUMENT_EMAILED",
        entity_type="SACE_DOCUMENT",
        details=f"Admin {action}ed '{title}'",
        ip_address=ip_addr
    )
    db.session.add(audit)
    db.session.commit()
    
    \2'''

text = re.sub(pattern_doc, replacement_doc, text)

# Replace in generate_auditor_code
# Look for: flash(f"New Auditor Access Code generated: {code}", "success")
pattern_gen = r'(flash\(f"New Auditor Access Code generated: \{code\}", "success"\))'
replacement_gen = r'''from app.models.core import CoreAuditEvent
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    audit = CoreAuditEvent(
        user_id=sace_user_id,
        action="CODE_GENERATED",
        entity_type="SACE_EVALUATOR_CODE",
        details=f"Generated new SACE Evaluator Access Code: {code}",
        ip_address=ip_addr
    )
    db.session.add(audit)
    db.session.commit()
    
    \1'''

text = re.sub(pattern_gen, replacement_gen, text)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
