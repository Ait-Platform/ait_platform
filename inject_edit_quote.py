import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

edit_route = '''@mechanic_bp.route("/mechanic/quote/<int:id>/edit", methods=["GET", "POST"])
@login_required
def edit_quote(id):
    from app.models.mechanic import MechJobCard, MechLaborLine, MechPartLine, MechCatalogPart, MechShop
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status != 'Quote':
        flash("Only Quotes can be edited. This job card has already progressed.", "warning")
        return redirect(url_for('mechanic_bp.job_card_detail', id=id))
        
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    
    if request.method == "POST":
        # Process dynamic labor and parts arrays
        labor_descs = request.form.getlist('labor_desc[]')
        labor_ins = request.form.getlist('labor_in[]')
        labor_outs = request.form.getlist('labor_out[]')
        labor_rates = request.form.getlist('labor_rate[]')

        part_qtys = request.form.getlist('part_qty[]')
        part_descs = request.form.getlist('part_desc[]')
        part_rates = request.form.getlist('part_rate[]')
        
        # Clear existing lines
        for l in job_card.labor_lines:
            db.session.delete(l)
        for p in job_card.part_lines:
            db.session.delete(p)
            
        # Add new lines
        for i in range(len(labor_descs)):
            desc = labor_descs[i].strip()
            if not desc:
                continue
            t_in = labor_ins[i] if i < len(labor_ins) else ""
            t_out = labor_outs[i] if i < len(labor_outs) else ""
            rate_str = labor_rates[i] if i < len(labor_rates) else "0"
            rate = float(rate_str) if rate_str else 0.0
            
            hours = 0.0
            if t_in and t_out:
                try:
                    h1, m1 = map(int, t_in.split(':'))
                    h2, m2 = map(int, t_out.split(':'))
                    diff = (h2 + m2/60.0) - (h1 + m1/60.0)
                    if diff < 0:
                        diff += 24.0
                    hours = round(diff, 2)
                except Exception:
                    pass

            labor = MechLaborLine(
                job_card_id=job_card.id,
                mechanic_name="Shop Tech",
                description=desc,
                time_in=t_in,
                time_out=t_out,
                hours=hours,
                rate_per_hour=rate
            )
            db.session.add(labor)
        
        for i in range(len(part_descs)):
            desc = part_descs[i].strip()
            if not desc:
                continue
            qty_str = part_qtys[i] if i < len(part_qtys) else "1"
            qty = int(qty_str) if qty_str else 1
            rate_str = part_rates[i] if i < len(part_rates) else "0"
            rate = float(rate_str) if rate_str else 0.0
            
            pline = MechPartLine(
                job_card_id=job_card.id,
                part_number="Custom/Selected",
                description=desc,
                quantity=qty,
                unit_cost=rate,
                markup_price=rate
            )
            db.session.add(pline)
            
        # Update vehicle details
        vehicle = job_card.vehicle
        if vehicle:
            vin_number = request.form.get("vin_number")
            make = request.form.get("make")
            model = request.form.get("model")
            year_str = request.form.get("year")
            year = int(year_str) if year_str and year_str.isdigit() else None
            mileage = request.form.get("mileage")
            
            if vin_number: vehicle.vin = vin_number
            if make: vehicle.make = make
            if model: vehicle.model = model
            if year: vehicle.year = year
            if mileage and mileage.isdigit(): vehicle.mileage = int(mileage)
            
            # Client details
            customer_name = request.form.get("customer_name")
            if customer_name and vehicle.client:
                vehicle.client.name = customer_name
                
        try:
            db.session.commit()
            flash("Quote updated successfully!", "success")
            return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))
        except Exception as e:
            db.session.rollback()
            flash(f"An error occurred while updating the quote. Error: {str(e)[:100]}", "danger")
            
    # GET Request: render form
    all_parts = MechCatalogPart.query.filter(
        (MechCatalogPart.user_id == None) | (MechCatalogPart.user_id == current_user.id)
    ).all()
    
    part_dict = {}
    for p in all_parts:
        name_lower = p.part_name.lower().strip()
        if name_lower not in part_dict:
            part_dict[name_lower] = p
        else:
            if p.user_id == current_user.id:
                part_dict[name_lower] = p
    
    catalog_parts = list(part_dict.values())
    catalog_parts.sort(key=lambda x: x.part_name)
    
    return render_template("program_mechanic/quote_form.html", catalog_parts=catalog_parts, shop=active_shop, edit_card=job_card)

'''

content = content.replace("def new_quote():", edit_route + "\n\n@mechanic_bp.route(\"/mechanic/quote/new\", methods=[\"GET\", \"POST\"])\n@login_required\ndef new_quote():")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
