import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

client_update_route = '''
@mechanic_bp.route("/mechanic/client/<int:client_id>/update", methods=["POST"])
@login_required
def update_client(client_id):
    from app.models.mechanic import MechClient
    from app.models.debtors import Debtor
    client = MechClient.query.get_or_404(client_id)
    job_id = request.args.get('job_id')
    
    name = request.form.get("name")
    phone = request.form.get("phone")
    email = request.form.get("email")
    
    if name:
        client.name = name
    client.phone = phone
    client.email = email
    
    # Sync with Debtors profile if it exists
    debtor = Debtor.query.filter_by(
        reference_id=client.id, slug_reference='mechanic', user_id=current_user.id).first()
    if debtor:
        if name:
            debtor.name = name
        debtor.phone = phone
        debtor.email = email
        
    db.session.commit()
    flash("Client details updated successfully.", "success")
    
    if job_id:
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_id))
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))
'''

# Insert it before job_card_detail
content = content.replace('@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])', client_update_route + '\n@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])')

# Update approve_quote route to redirect to mechanic_bp.job_cards_list instead of job_card_detail
approve_original = '''    flash("Proof of Payment captured! Document converted to Tax Invoice.", "success")
    return redirect(url_for("mechanic_bp.job_card_detail", id=id))'''

approve_new = '''    flash("Proof of Payment captured! Document converted to Tax Invoice.", "success")
    return redirect(url_for("mechanic_bp.job_cards_list"))'''

content = content.replace(approve_original, approve_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py with update_client route and approve_quote redirect")
