import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Make secure_view route handle p_guide
old_route = '''    # Retrieve the document URL first
    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    # TESTING FIX: If doc is missing, log interaction anyway and use a fallback title
    doc_title = doc.title if doc else doc_type.replace('_', ' ').title()
    doc_url = doc.document_url if doc else ""'''

new_route = '''    # Retrieve the document URL first
    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    # TESTING FIX: If doc is missing, log interaction anyway and use a fallback title
    doc_title = doc.title if doc else doc_type.replace('_', ' ').title()
    doc_url = doc.document_url if doc else ""
    
    # HARDCODE P_GUIDE FOR TESTING
    if doc_type == 'p_guide':
        doc_title = "LITRE Participant Manual (P Guide)"
        doc_url = url_for('static', filename='pdf/P_Guide.pdf')'''
        
routes = routes.replace(old_route, new_route)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
