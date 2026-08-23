import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

route_logic = '''
@mechanic_bp.route("/mechanic/job_card/<int:id>/payment_method", methods=["POST"])
@login_required
def update_payment_method(id):
    from app.models.mechanic import MechJobCard
    job_card = MechJobCard.query.get_or_404(id)
    
    # Ensure ownership
    if job_card.vehicle.client.user_id != current_user.id:
        flash("Unauthorized", "danger")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
        
    pm = request.form.get("payment_method")
    if pm in ['EFT', 'eWallet', 'Cash']:
        job_card.payment_method = pm
        db.session.commit()
        flash(f"Payment method updated to {pm}.", "success")
        
    return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
'''

content = content + "\n" + route_logic

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
