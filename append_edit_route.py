with open('app/program_mechanic/routes.py', 'a', encoding='utf-8') as f:
    f.write('''

@mechanic_bp.route("/mechanic/job_card/<int:id>/edit_vehicle", methods=["POST"])
@login_required
def edit_job_vehicle(id):
    from app.models.mechanic import MechJobCard
    job_card = MechJobCard.query.get_or_404(id)
    
    # Ensure ownership
    if job_card.vehicle.client.shop.user_id != current_user.id:
        flash("Unauthorized", "error")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
        
    job_card.vehicle.make = request.form.get("make")
    job_card.vehicle.model = request.form.get("model")
    job_card.vehicle.vin = request.form.get("vin")
    
    mileage_str = request.form.get("mileage")
    if mileage_str and mileage_str.isdigit():
        job_card.vehicle.mileage = int(mileage_str)
        
    job_card.next_service_due = request.form.get("next_service_due")
    
    db.session.commit()
    flash("Vehicle details updated successfully.", "success")
    return redirect(url_for('mechanic_bp.job_card_view', id=job_card.id))
''')
