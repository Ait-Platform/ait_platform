import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

# I will append the new routes at the end of the file.
new_routes = '''
# ==========================================
# SACE CONTROL CENTRE: DOCUMENTS & LOGS
# ==========================================

@sace_bp.route("/sace/provisioning/documents")
@login_required
def provider_documents():
    from app.models.sace import SaceWorkshopInteraction
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Check interaction tracking
    interactions = SaceWorkshopInteraction.query.filter_by(
        user_id=sace_user_id
    ).filter(SaceWorkshopInteraction.activity_slug.like("doc_tracked_%")).all()
    
    tracked_ids = [inv.activity_slug.replace("doc_tracked_", "") for inv in interactions]
    
    documents = [
        {
            "id": "1",
            "title": "Provider Application Form",
            "description": "General application form for provider approval.",
            "is_tracked": "1" in tracked_ids
        },
        {
            "id": "2",
            "title": "Professional Development Activity Form for 2 Hours TO 5 Days Programs (1)",
            "description": "Application form for short duration activities.",
            "is_tracked": "2" in tracked_ids
        },
        {
            "id": "3",
            "title": "Professional Development Activity Application form of duration from 6 days upwards",
            "description": "Application form for long duration activities.",
            "is_tracked": "3" in tracked_ids
        },
        {
            "id": "4",
            "title": "Facilitator CVs",
            "description": "Curriculum Vitae of the programme facilitators.",
            "is_tracked": "4" in tracked_ids
        }
    ]
    
    return render_template("program_sace/provider_documents_map.html", documents=documents)


@sace_bp.route("/sace/provisioning/document/action/<doc_id>", methods=["GET", "POST"])
@login_required
def document_action(doc_id):
    from app.models.sace import SaceWorkshopInteraction
    from app.utils.mailer import send_email
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    action = request.args.get('action') or request.form.get('action')
    
    # 1. Log the tracking interaction if not already logged
    slug = f"doc_tracked_{doc_id}"
    existing = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug=slug).first()
    if not existing:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug=slug,
            response_data=json.dumps({"action": action, "doc_id": doc_id})
        )
        db.session.add(interaction)
        db.session.commit()
        
    doc_file_map = {
        "1": "pdf/App_Form_1.pdf",
        "2": "pdf/App_Form_1.pdf", # Same dummy file for now
        "3": "pdf/App_Form_2.pdf",
        "4": "pdf/Facilitator_CVs.pdf"
    }
    
    filename = doc_file_map.get(doc_id, "pdf/App_Form_1.pdf")
    
    if action == "view":
        return redirect(url_for('static', filename=filename))
        
    elif action == "email" and request.method == "POST":
        recipient = request.form.get("recipient_email")
        if recipient:
            doc_names = {
                "1": "Provider Application Form",
                "2": "Professional Development Activity Form (2-5 Days)",
                "3": "Professional Development Activity Form (6+ Days)",
                "4": "Facilitator CVs"
            }
            doc_name = doc_names.get(doc_id, "Document")
            
            html_body = f"""
            <div style="font-family: sans-serif; padding: 20px;">
                <h2 style="color: #4f46e5;">AIT Provider Document</h2>
                <p>Hello,</p>
                <p>Please find the requested AIT SACE Document: <strong>{doc_name}</strong>.</p>
                <p>You can securely access and download this document using the link below:</p>
                <div style="margin: 20px 0;">
                    <a href="{url_for('static', filename=filename, _external=True)}" style="background-color: #4f46e5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold;">View Document</a>
                </div>
                <p>Regards,<br>AIT Administrator</p>
            </div>
            """
            send_email(
                subject=f"AIT Document: {doc_name}",
                recipients=[recipient],
                body=f"Please view the document here: {url_for('static', filename=filename, _external=True)}",
                html=html_body
            )
            flash(f"Document securely emailed to {recipient}.", "success")
            
        return redirect(url_for('sace_bp.provider_documents'))
        
    return redirect(url_for('sace_bp.provider_documents'))


@sace_bp.route("/sace/provisioning/logs")
@login_required
def provisioning_logs():
    from app.models.core import CoreAuditEvent
    from app.models.sace import SaceWorkshopInteraction
    from app.models.auth import User
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Find auditor emails provisioned by this user
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").all()
    emails = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            if 'email' in data:
                emails.append(data['email'].lower())
        except:
            pass
            
    auditor_ids = []
    if emails:
        auditor_users = User.query.filter(db.func.lower(User.email).in_(emails)).all()
        auditor_ids = [u.id for u in auditor_users]
        
    target_ids = [sace_user_id] + auditor_ids
    
    # Query logs for these users
    events = CoreAuditEvent.query.filter(CoreAuditEvent.user_id.in_(target_ids)).order_by(CoreAuditEvent.created_at.desc()).limit(100).all()
    
    # We will reuse the audit_report template but with a back button context if we want, 
    # but the simplest is just to render it natively with these events.
    # The existing template might need a slight tweak to show a back button to Control Centre.
    return render_template("program_sace/compliance/audit_report.html", events=events, is_control_centre=True)

'''

with open(routes_path, 'a', encoding='utf-8') as f:
    f.write(new_routes)

print("Routes added.")
