import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

email_original = '''@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])
@login_required
def email_document(id):
    from app.utils.mailer import send_email
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

        subject = f"Your {doc_type} #{job_card.job_number} from AIT ProTrade"
        doc_url = url_for('mechanic_bp.job_card_detail', id=id, _external=True)

        body = f"Hello,\n\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\n\nThank you for choosing us!"
        html = f"<p>Hello,</p><p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can view it here: <a href='{doc_url}'>{doc_url}</a></p><p>Thank you for choosing us!</p>"

        success = send_email(subject=subject, recipients=[
                             target_email], body=body, html=html)

        if success:
            flash(f"{doc_type} successfully emailed to {target_email}", "success")
        else:
            flash("Failed to send email. Please check server logs.", "danger")

        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

    return render_template("program_mechanic/email_preview.html", job_card=job_card, doc_type=doc_type, default_email=default_email)'''


email_new = '''@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])
@login_required
def email_document(id):
    from app.utils.mailer import send_pdf_email
    from app.utils.pdf_render import html_to_pdf_bytes
    from flask import current_app
    
    job_card = MechJobCard.query.get_or_404(id)
    shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()

    doc_type = "Statement of Account (SOA)" if job_card.status in ['Billed', 'Approved'] else "Quote / Tax Invoice"
    file_name = f"{'SOA' if job_card.status in ['Billed', 'Approved'] else 'Quote'}_{job_card.job_number}.pdf"
    
    default_email = ""
    if job_card.vehicle and job_card.vehicle.client and job_card.vehicle.client.email:
        default_email = job_card.vehicle.client.email

    if request.method == "POST":
        target_email = request.form.get("email")
        if not target_email:
            flash("Please provide an email address.", "warning")
            return redirect(url_for('mechanic_bp.email_document', id=id))

        subject = f"Your {doc_type} #{job_card.job_number} from {shop.business_name if shop else 'us'}"
        
        # Render the PDF
        html_content = render_template("program_mechanic/invoice_view.html", job_card=job_card, shop=shop)
        
        try:
            pdf_bytes = html_to_pdf_bytes(html_content, base_url=request.host_url)
            
            body = f"Hello,\n\nPlease find attached your {doc_type} #{job_card.job_number}.\n\nThank you for choosing us!"
            
            send_pdf_email(
                to_email=target_email,
                subject=subject,
                body_text=body,
                pdf_bytes=pdf_bytes,
                filename=file_name
            )
            flash(f"{doc_type} successfully emailed to {target_email} with PDF attachment.", "success")
        except Exception as e:
            current_app.logger.error(f"Failed to generate/send PDF: {e}")
            flash("Failed to generate and send PDF email. Please check server logs.", "danger")

        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

    return render_template("program_mechanic/email_preview.html", job_card=job_card, doc_type=doc_type, default_email=default_email)'''

content = content.replace(email_original, email_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated email_document successfully")
