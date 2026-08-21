import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

email_func_regex = re.compile(r'@mechanic_bp\.route\("/mechanic/email/<int:id>", methods=\["GET", "POST"\]\)\n@login_required\ndef email_document\(id\):.*?return render_template\("program_mechanic/email_preview\.html", job_card=job_card, doc_type=doc_type, default_email=default_email\)', re.DOTALL)

new_email_func = '''@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])
@login_required
def email_document(id):
    from app.utils.mailer import send_email, send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    from datetime import datetime
    from flask_mail import Message
    from app.extensions import mail
    
    job_card = MechJobCard.query.get_or_404(id)

    doc_type = "Invoice" if job_card.status == 'Billed' else "Quote"
    default_email = ""
    if job_card.vehicle and job_card.vehicle.client and job_card.vehicle.client.email:
        default_email = job_card.vehicle.client.email

    if request.method == "POST":
        target_email = request.form.get("email")
        if not target_email:
            flash("Please provide an email address.", "warning")
            return redirect(url_for('mechanic_bp.email_document', id=id))

        active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
        subject = f"Your {doc_type} #{job_card.job_number} from {active_shop.business_name if active_shop else 'AIT ProTrade'}"
        
        # VERY IMPORTANT: doc_url must be the public job card URL
        doc_url = url_for('mechanic_bp.public_job_card', job_number=job_card.job_number, _external=True)

        body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\\n\\nWe have also attached a PDF copy for your records.\\n\\nThank you for choosing us!"
        
        # Prepare HTML Email Body
        letterhead_html = ""
        if active_shop and active_shop.use_custom_letterhead and active_shop.letterhead_url:
            lh_url = url_for('static', filename=f'uploads/mechanic/{active_shop.letterhead_url}', _external=True)
            letterhead_html = f'<div style="text-align: center; margin-bottom: 20px;"><img src="{lh_url}" alt="Shop Letterhead" style="max-width: 100%; height: auto; max-height: 150px; border-radius: 8px;"></div><hr style="border: 0; border-top: 1px solid #e2e8f0; margin-bottom: 20px;">'

        html = f"""{letterhead_html}
        <div style="font-family: sans-serif; color: #334155; max-width: 600px; margin: 0 auto;">
            <p>Hello,</p>
            <p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can securely view and download it here:</p>
            <p style="text-align: center; margin: 30px 0;">
                <a href='{doc_url}' style="background-color: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">View {doc_type}</a>
            </p>
            <p>We have also attached a PDF copy for your convenience.</p>
            <p>Thank you for choosing us!</p>
        </div>"""

        # Generate PDF Attachment
        today_date = datetime.utcnow().strftime('%Y-%m-%d')
        pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date)
        
        success = False
        try:
            pdf_bytes = html_to_pdf_bytes(pdf_html_content, base_url=request.host_url)
            file_name = f"{'Invoice' if job_card.status == 'Billed' else 'Quote'}_{job_card.job_number}.pdf"
            
            msg = Message(subject=subject, recipients=[target_email], body=body, html=html)
            msg.sender = current_app.config.get("MAIL_DEFAULT_SENDER")
            msg.attach(file_name, "application/pdf", pdf_bytes)
            mail.send(msg)
            success = True
        except Exception as e:
            current_app.logger.error(f"Failed to generate/send PDF: {e}")
            # Fallback to standard email without attachment
            success = send_email(subject=subject, recipients=[target_email], body=body, html=html)

        if success:
            from app.models.mechanic import MechCommunication
            from app.models.auth import InviteLog
            
            comm = MechCommunication(
                job_card_id=job_card.id,
                comm_type="Email",
                recipient=target_email,
                message=f"Sent {doc_type} #{job_card.job_number}",
                status="Success"
            )
            db.session.add(comm)
            
            phone = "Unknown Client"
            if job_card.vehicle and job_card.vehicle.client:
                phone = job_card.vehicle.client.phone or f"{job_card.vehicle.client.name} (Client)"
            
            ilog = InviteLog(
                sender_id=current_user.id,
                recipient_phone=phone,
                program_slug="mechanic",
                invite_type=f"Email {doc_type} #{job_card.job_number}",
                status="Sent"
            )
            db.session.add(ilog)
            db.session.commit()
            
            flash(f"{doc_type} successfully emailed to {target_email}", "success")
        else:
            flash("Failed to send email. Please check server logs.", "danger")

        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

    return render_template("program_mechanic/email_preview.html", job_card=job_card, doc_type=doc_type, default_email=default_email)'''

if email_func_regex.search(content):
    content = email_func_regex.sub(new_email_func, content)
    print("SUCCESS")
else:
    print("FAILED")
    
with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
