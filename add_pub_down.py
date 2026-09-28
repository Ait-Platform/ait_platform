import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

route_code = """
@uip_bp.route("/<org_slug>/mandates/<int:doc_id>/download")
def download_mandate_proof(org_slug, doc_id):
    from app.models.core import CoreOrganization
    from app.models.uip import UipDocument
    import os
    from flask import current_app, send_file
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    doc = UipDocument.query.filter_by(organization_id=org.id, id=doc_id).first_or_404()
    
    file_path = os.path.join(current_app.instance_path, "uip_documents", str(org.id), f"{doc.id}_{doc.filename}")
    if not os.path.exists(file_path):
        abort(404)
        
    return send_file(file_path, as_attachment=False, download_name=doc.filename)
"""

content += "\n" + route_code

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added public download route")
