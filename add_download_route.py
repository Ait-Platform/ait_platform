import sys

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

download_route = '''
@mechanic_bp.route("/mechanic/download/<int:id>", methods=["GET"])
@login_required
def download_document(id):
    from app.utils.pdf_render import html_to_pdf_bytes
    from datetime import datetime
    import io
    from flask import send_file
    
    job_card = MechJobCard.query.get_or_404(id)
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    pdf_html_content = render_template("program_mechanic/public_job_card.html", job_card=job_card, shop=active_shop, today_date=today_date)
    
    try:
        pdf_bytes = html_to_pdf_bytes(pdf_html_content, base_url=request.host_url)
        doc_type = "Invoice" if job_card.status == 'Billed' else "Quote"
        file_name = f"{doc_type}_{job_card.job_number}.pdf"
        
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=file_name
        )
    except Exception as e:
        current_app.logger.error(f"Failed to generate PDF: {e}")
        flash("Failed to generate PDF. Please try again.", "danger")
        return redirect(url_for('mechanic_bp.job_card_detail', id=id))

'''

if "def download_document(id):" not in content:
    target = '''@mechanic_bp.route("/mechanic/email/<int:id>", methods=["GET", "POST"])'''
    content = content.replace(target, download_route + target)
    with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added download_document route!")
else:
    print("download_document already exists.")
