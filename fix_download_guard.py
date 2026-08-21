import sys

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    job_card = MechJobCard.query.get_or_404(id)
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()'''

new_target = '''    job_card = MechJobCard.query.get_or_404(id)
    
    client = job_card.vehicle.client if job_card.vehicle else None
    if (client and (not client.email or not client.phone)) or (job_card.vehicle and not job_card.vehicle.vin):
        flash("Please fill in the client's email, phone, and vehicle VIN before generating documents.", "warning")
        return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
        
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()'''

content = content.replace(target, new_target)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
