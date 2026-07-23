import sys

with open('D:/Users/yeshk/Documents/ait_platform/app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re
old_quote_post = '''        if new_part_name and new_part_price:
            learned_part = MechCatalogPart(
                user_id=current_user.id,
                part_name=new_part_name,
                category='Custom',
                default_price=float(new_part_price)
            )
            db.session.add(learned_part)
            db.session.commit()
            flash(f"Learned new part: {new_part_name}", "success")
            
        flash("Quote created successfully!", "success")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))'''

new_quote_post = '''        selected_part_ids = request.form.getlist('selected_parts')
        
        if new_part_name and new_part_price:
            learned_part = MechCatalogPart(
                user_id=current_user.id,
                part_name=new_part_name,
                category='Custom',
                default_price=float(new_part_price)
            )
            db.session.add(learned_part)
            db.session.flush()
            selected_part_ids.append(str(learned_part.id))
            flash(f"Learned new part: {new_part_name}", "success")
            
        from app.models.mechanic import MechClient, MechVehicle, MechJobCard, MechPartLine, MechLaborLine
        import uuid
        
        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()
        if not client:
            client = MechClient(name=customer_name)
            db.session.add(client)
            db.session.flush()
            
        vehicle = MechVehicle.query.filter_by(registration_number=vehicle_reg, client_id=client.id).first()
        if not vehicle:
            vehicle = MechVehicle(registration_number=vehicle_reg, make="Unknown", client_id=client.id)
            db.session.add(vehicle)
            db.session.flush()
            
        job_card = MechJobCard(
            job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",
            vehicle_id=vehicle.id,
            status='Ready'
        )
        db.session.add(job_card)
        db.session.flush()
        
        # Add a default labor line
        labor = MechLaborLine(job_card_id=job_card.id, mechanic_name="General", description="General Inspection", hours=1.0, rate_per_hour=350.0)
        db.session.add(labor)
        
        for p_id in selected_part_ids:
            part_def = MechCatalogPart.query.get(p_id)
            if part_def:
                pline = MechPartLine(
                    job_card_id=job_card.id,
                    part_number=part_def.part_name,
                    description=f"{part_def.category} part",
                    quantity=1,
                    unit_cost=part_def.default_price,
                    markup_price=part_def.default_price
                )
                db.session.add(pline)
                
        db.session.commit()
            
        flash("Quote created and Job Card generated successfully!", "success")
        return redirect(url_for("mechanic_bp.job_card_detail", id=job_card.id))'''

if old_quote_post in content:
    content = content.replace(old_quote_post, new_quote_post)
    with open('D:/Users/yeshk/Documents/ait_platform/app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated routes.py")
else:
    print("Could not find the target code in routes.py")
