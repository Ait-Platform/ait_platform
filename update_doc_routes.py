import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''@mechanic_bp.route("/mechanic/job_card/<int:id>/download")
def download_document(id):
    from weasyprint import HTML, CSS
    from app.models.debtors import BusinessBankAccount
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.vehicle.client.user_id != current_user.id:
        flash("Access denied", "danger")
        return redirect(url_for('mechanic_bp.job_cards_list'))
        
    shop = MechShop.query.filter_by(user_id=current_user.id).first()
    bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not bank_account:
        bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).first()
        
    html_out = render_template('program_mechanic/public_job_card.html', 
                             job_card=job_card,
                             shop=shop,
                             bank_account=bank_account)'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/job_card/<int:id>/download\"\)\s*def download_document\(id\):\s*from weasyprint import HTML, CSS\s*job_card = MechJobCard\.query\.get_or_404\(id\)\s*if job_card\.vehicle\.client\.user_id != current_user\.id:\s*flash\(\"Access denied\", \"danger\"\)\s*return redirect\(url_for\('mechanic_bp\.job_cards_list'\)\)\s*shop = MechShop\.query\.filter_by\(user_id=current_user\.id\)\.first\(\)\s*html_out = render_template\('program_mechanic/public_job_card\.html', \s*job_card=job_card,\s*shop=shop\)",
    replacement,
    content
)

# Do the same for email_document
replacement2 = '''@mechanic_bp.route("/mechanic/job_card/<int:id>/email")
def email_document(id):
    from weasyprint import HTML, CSS
    from app.models.debtors import BusinessBankAccount
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.vehicle.client.user_id != current_user.id:
        flash("Access denied", "danger")
        return redirect(url_for('mechanic_bp.job_cards_list'))

    client_email = job_card.vehicle.client.email
    if not client_email:
        flash("Client does not have an email address saved.", "danger")
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
        
    shop = MechShop.query.filter_by(user_id=current_user.id).first()
    bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id, is_default=True).first()
    if not bank_account:
        bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id).first()

    html_out = render_template('program_mechanic/public_job_card.html', 
                             job_card=job_card,
                             shop=shop,
                             bank_account=bank_account)'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/job_card/<int:id>/email\"\)\s*def email_document\(id\):\s*from weasyprint import HTML, CSS\s*job_card = MechJobCard\.query\.get_or_404\(id\)\s*if job_card\.vehicle\.client\.user_id != current_user\.id:\s*flash\(\"Access denied\", \"danger\"\)\s*return redirect\(url_for\('mechanic_bp\.job_cards_list'\)\)\s*client_email = job_card\.vehicle\.client\.email\s*if not client_email:\s*flash\(\"Client does not have an email address saved\.\", \"danger\"\)\s*return redirect\(url_for\('mechanic_bp\.job_card_detail', id=job_card\.id\)\)\s*shop = MechShop\.query\.filter_by\(user_id=current_user\.id\)\.first\(\)\s*html_out = render_template\('program_mechanic/public_job_card\.html', \s*job_card=job_card,\s*shop=shop\)",
    replacement2,
    content
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
