import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

route_logic = '''
@mechanic_bp.route("/mechanic/job_card/<int:id>/quick_odometer", methods=["POST"])
@login_required
def quick_update_odometer(id):
    from app.models.mechanic import MechJobCard
    job_card = MechJobCard.query.get_or_404(id)
    
    if job_card.vehicle.client.user_id != current_user.id:
        flash("Unauthorized", "danger")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
        
    mileage_str = request.form.get('mileage')
    try:
        mileage = int(mileage_str)
        job_card.vehicle.mileage = mileage
        job_card.next_service_due = f"{mileage + 15000:,.0f} km"
        db.session.commit()
        flash("Odometer updated successfully.", "success")
    except ValueError:
        flash("Invalid odometer reading.", "danger")
        
    return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
'''

content = content.replace("@mechanic_bp.route(\"/mechanic/job_card/<int:id>/edit_vehicle\", methods=[\"POST\"])", route_logic + "\n@mechanic_bp.route(\"/mechanic/job_card/<int:id>/edit_vehicle\", methods=[\"POST\"])")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
