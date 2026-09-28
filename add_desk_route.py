import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

route_code = """
@uip_bp.route("/<org_slug>/mandate-recording-desk", methods=["GET", "POST"])
@login_required
def mandate_recording_desk(org_slug):
    from app.models.core import CoreOrganization
    from app.models.uip import UipResolution, UipDocument
    from app.models.uip_governance import UipCommitteeMember
    from sqlalchemy import func
    import os
    from flask import current_app
    from werkzeug.utils import secure_filename
    from datetime import datetime
    
    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    # Require Secretary (or Chairman/Vice)
    current_appointment = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
    ).first()
    
    if not current_appointment or current_appointment.position.lower() not in ["chairman", "chairperson", "chair", "vice chair", "vice chairman", "secretary"]:
        abort(403)
        
    if request.method == "POST":
        title_selection = request.form.get("title_selection")
        
        # Determine if it's a new foundational mandate or an existing resolution
        res = None
        if title_selection.startswith("RES_"):
            res_id = int(title_selection.split("_")[1])
            res = UipResolution.query.filter_by(organization_id=org.id, id=res_id).first_or_404()
        else:
            # Create new Foundational Mandate
            res = UipResolution(
                organization_id=org.id,
                creator_id=current_user.id,
                title=title_selection,
                description="Please refer to the official attached mandate document for full details.",
                status="ADOPTED",
                voting_scope="PUBLIC",
                decision_date=datetime.utcnow().date(),
                reference="FOUNDATIONAL",
                yea_tally=0,
                nay_tally=0
            )
            db.session.add(res)
            db.session.flush() # Get ID
            
        res.status = "ADOPTED"
        res.description = "Please refer to the official attached mandate document for full details."
        
        # Base ratification json
        basis = res.result_basis or {}
        basis["ratification"] = {
            "date": str(datetime.utcnow().date()),
            "recorded_by_name": current_user.name,
            "recorded_by_email": current_user.email,
            "endorsements": []
        }
        res.result_basis = basis
        
        # Process PDF upload
        if "mandate_file" in request.files:
            f = request.files["mandate_file"]
            if f and f.filename:
                filename = secure_filename(f.filename)
                upload_dir = os.path.join(current_app.instance_path, "uip_documents", str(org.id))
                os.makedirs(upload_dir, exist_ok=True)
                
                doc_record = UipDocument(
                    organization_id=org.id,
                    resolution_id=res.id,
                    title=f"Proof: {res.title}",
                    category="MANDATE",
                    access_classification="public",
                    uploaded_by=current_user.id,
                    effective_date=datetime.utcnow().date(),
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
        flash(f"Mandate for '{res.title}' successfully recorded and saved to register.", "success")
        return redirect(url_for("uip_bp.mandate_recording_desk", org_slug=org.slug))
        
    # GET: Populate form
    # Fetch pending resolutions (Tabled or Proposed)
    pending_resolutions = UipResolution.query.filter(
        UipResolution.organization_id == org.id,
        UipResolution.status.in_(["TABLED", "PROPOSED"])
    ).order_by(UipResolution.created_at.desc()).all()
    
    # Calculate next mandate number (pseudo)
    adopted_count = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").count()
    next_mnd = f"MND-{datetime.utcnow().year}-{(adopted_count + 1):03d}"
    
    return render_template(
        "program_uip/dashboards/mandate_recording_desk.html",
        org=org,
        pending_resolutions=pending_resolutions,
        next_mnd=next_mnd
    )
"""

content += "\n" + route_code

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route added")
