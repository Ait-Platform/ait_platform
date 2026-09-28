import re

filepath = 'app/program_uip/committee_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update decide_resolution logic
decision_pattern = re.compile(
    r'(if decision == "ADOPTED":.*?res\.status = decision)',
    re.DOTALL
)

new_decision_logic = """if decision == "ADOPTED":
        # 1. Capture meeting details
        meeting_date_str = request.form.get("meeting_date")
        meeting_location = request.form.get("meeting_location")
        live_yea = request.form.get("live_yea", type=int, default=0)
        live_nay = request.form.get("live_nay", type=int, default=0)
        live_abstain = request.form.get("live_abstain", type=int, default=0)
        
        if meeting_date_str:
            from datetime import date
            try:
                res.decision_date = date.fromisoformat(meeting_date_str)
            except ValueError:
                pass
                
        # Store live tallies and location in result_basis
        basis = res.result_basis or {}
        basis.update({
            "meeting_location": meeting_location,
            "live_yea": live_yea,
            "live_nay": live_nay,
            "live_abstain": live_abstain,
            "endorsements": [] # Setup for the new safeguard feature
        })
        res.result_basis = basis
        
        # 2. Handle optional Mandate Proof (PDF)
        if "mandate_file" in request.files:
            f = request.files["mandate_file"]
            if f and f.filename:
                import os
                from flask import current_app
                from werkzeug.utils import secure_filename
                from app.models.uip import UipDocument
                
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
                    effective_date=res.decision_date or db.func.current_date(),
                    filename=filename,
                    size_bytes=0,
                    current_version=1
                )
                db.session.add(doc_record)
                db.session.flush()
                
                file_path = os.path.join(upload_dir, f"{doc_record.id}_{filename}")
                f.save(file_path)
                doc_record.size_bytes = os.path.getsize(file_path)

        votes = res.votes.all() if hasattr(res, 'votes') else []
        scope = getattr(res, 'voting_scope', 'EXCO')
        
        if scope in ['EXCO', 'EXCO_CORE', 'COMMITTEE_ALL', 'SUB_COMMITTEE']:
            # Get all eligible committee members
            if scope == 'EXCO_CORE':
                eligible_members = UipCommitteeMember.query.filter(
                    UipCommitteeMember.organization_id == org.id, 
                    UipCommitteeMember.status == "CURRENT",
                    UipCommitteeMember.position.in_(["Chairperson", "Vice-Chairperson", "Secretary", "Treasurer"])
                ).all()
            else:
                eligible_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
            
            voted_user_ids = [v.user_id for v in votes]
            missing_members = [m.name for m in eligible_members if m.user_id not in voted_user_ids]
            
            if missing_members:
                missing_names = ", ".join(missing_members)
                flash(f"Resolution proceeded. WARNING: The following elected members did not cast a digital vote before ratification: {missing_names}", "warning")
            else:
                flash("Resolution proceeded. All elected members successfully cast their digital votes prior to ratification.", "success")
                
        else:
            # For PUBLIC/Ratepayer scopes
            from app.models.core import CoreOrganizationMember
            total_eligible = CoreOrganizationMember.query.filter_by(organization_id=org.id, is_active=True).count()
            if total_eligible == 0: total_eligible = 1
            current_quorum_pct = int((len(votes) / total_eligible) * 100)
            flash(f"Public vote reached {current_quorum_pct}% participation. Proceeding to live ratification.", "info")
            
    res.status = decision"""

content = decision_pattern.sub(new_decision_logic, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("decide_resolution patched")
