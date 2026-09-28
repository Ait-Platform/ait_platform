import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

route_code = """
@uip_bp.route("/<org_slug>/record-foundational-mandate", methods=["POST"])
@login_required
def record_foundational_mandate(org_slug):
    from app.models.core import CoreOrganization
    from app.models.uip import UipResolution, UipDocument
    from datetime import date
    import os
    from flask import current_app
    from werkzeug.utils import secure_filename
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    if not _require_role("secretary", abort_on_fail=False) and not _require_role("manager", abort_on_fail=False):
        abort(403)
        
    title = request.form.get("title")
    description = request.form.get("description")
    meeting_date_str = request.form.get("meeting_date")
    meeting_location = request.form.get("meeting_location")
    
    meeting_date = date.today()
    if meeting_date_str:
        try:
            meeting_date = date.fromisoformat(meeting_date_str)
        except ValueError:
            pass
            
    # Create the Mandate (ADOPTED Resolution)
    mandate = UipResolution(
        organization_id=org.id,
        creator_id=current_user.id,
        title=title,
        description=description,
        status="ADOPTED",
        voting_scope="PUBLIC",
        decision_date=meeting_date,
        reference="FOUNDATIONAL MANDATE",
        yea_tally=0,
        nay_tally=0
    )
    db.session.add(mandate)
    db.session.flush()
    
    # Handle optional file upload
    if "mandate_file" in request.files:
        f = request.files["mandate_file"]
        if f and f.filename:
            filename = secure_filename(f.filename)
            upload_dir = os.path.join(current_app.instance_path, "uip_documents", str(org.id))
            os.makedirs(upload_dir, exist_ok=True)
            
            doc_record = UipDocument(
                organization_id=org.id,
                resolution_id=mandate.id,
                title=f"Proof: {title}",
                category="MANDATE",
                access_classification="public",
                uploaded_by=current_user.id,
                effective_date=meeting_date,
                filename=filename,
                size_bytes=0,
                current_version=1
            )
            db.session.add(doc_record)
            db.session.flush()
            
            file_path = os.path.join(upload_dir, f"{doc_record.id}_{filename}")
            f.save(file_path)
            doc_record.size_bytes = os.path.getsize(file_path)
            
    db.session.commit()
    flash(f"Foundational Mandate '{title}' successfully recorded.", "success")
    return redirect(url_for("uip_bp.committee_dashboard", org_slug=org.slug, view="register"))
"""

# Append it to the file or just insert it somewhere safe
# We can just append it
content += "\n" + route_code

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("committee_routes updated")
