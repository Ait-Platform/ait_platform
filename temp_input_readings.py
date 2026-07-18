def input_readings(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        abort(403)
        
    units = BilSectionalUnit.query.filter_by(property_id=prop.id).all()
    
    # Collect all meters attached to any unit in this property
    all_meters = []
    for u in units:
        all_meters.extend(u.meters)
        
    # Also collect master meters by municipal account numbers
    from app.models.billing import BilMuniAccount, BilMeter
    muni_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    # Also grab meters directly mapped by water_meter_id / elec_meter_id if any exist
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
        
    from datetime import datetime
    import calendar
    
    if request.method == "POST":
        reading_month = request.form.get("reading_month")
        if not reading_month:
            flash("Reading month is required.", "danger")
            return redirect(request.url)
            
        added_count = 0
        for m in all_meters:
            val_str = request.form.get(f"reading_{m.id}")
            date_str = request.form.get(f"date_{m.id}")
            
            # Handle Baseline (Previous) Reading if submitted
            prev_val_str = request.form.get(f"prev_reading_{m.id}")
            prev_date_str = request.form.get(f"prev_date_{m.id}")
            
            # If the user provided a baseline, save it first
            if prev_val_str and prev_val_str.strip() and prev_date_str and prev_date_str.strip():
                prev_val = float(prev_val_str)
                prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
                
                # Create the baseline reading
                baseline_read_obj = BilMeterReading(
                    meter_id=m.id,
                    reading_date=prev_date,
                    reading_value=prev_val
                )
                db.session.add(baseline_read_obj)
                db.session.flush() # Ensure it's available for the new reading logic below
            
            if val_str and val_str.strip() and date_str and date_str.strip():
                new_val = float(val_str)
                new_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                # Fetch the latest reading before or equal to this new_date
                last_reading = BilMeterReading.query.filter(
                    BilMeterReading.meter_id == m.id,
                    BilMeterReading.reading_date <= new_date # Allow same day if baseline was just added
                ).order_by(BilMeterReading.reading_date.desc()).first()
                
                # Prevent duplicate entries on exact same day if it's the exact same value
                if last_reading and last_reading.reading_date == new_date and last_reading.reading_value == new_val:
                    continue
                
                # Create the new reading
                new_read_obj = BilMeterReading(
                    meter_id=m.id,
                    reading_date=new_date,
                    reading_value=new_val
                )
                db.session.add(new_read_obj)
                
                # If there is a previous reading, calculate consumption
                if last_reading and last_reading.reading_date < new_date:
                    days = (new_date - last_reading.reading_date).days
                    if days > 0:
                        consumption_val = new_val - last_reading.reading_value
                        
                        # Only save positive consumption (if meter rolled over or replaced, needs manual adjustment)
                        if consumption_val >= 0:
                            cons_obj = BilConsumption(
                                meter_id=m.id,
                                meter_number=m.meter_number,
                                last_date=last_reading.reading_date,
                                new_date=new_date,
                                last_read=last_reading.reading_value,
                                new_read=new_val,
                                days=days,
                                consumption=consumption_val,
                                month=reading_month
                            )
                            db.session.add(cons_obj)
                added_count += 1
                
        if added_count > 0:
            db.session.commit()
            flash(f"Successfully saved {added_count} meter reading(s) for {reading_month}!", "success")
            
            # Redirect to the Property Hub
            return redirect(url_for('billing_bp.property_hub', property_id=property_id))
        else:
            flash("No readings were entered.", "warning")
            
        return redirect(url_for('billing_bp.property_hub', property_id=property_id))
        
    # GET: Prepare data for template
    passed_date = request.args.get('date')
    if passed_date:
        try:
            # Parse just to validate format, then use it
            dt = datetime.strptime(passed_date, "%Y-%m-%d")
            current_date = passed_date
            current_month = dt.strftime("%Y-%m")
        except ValueError:
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_month = datetime.now().strftime("%Y-%m")
    else:
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_month = datetime.now().strftime("%Y-%m")
        
    meters_data = []
    for m in all_meters:
        # Determine hierarchy
        is_bulk = False
        for om in all_meters:
            if om.parent_meter_id == m.id:
                is_bulk = True
                break
        hierarchy = 'Bulk' if is_bulk else ('Sub-Meter' if m.parent_meter_id else 'Independent')
        
        # Get latest reading
        last_reading = BilMeterReading.query.filter_by(meter_id=m.id).order_by(BilMeterReading.reading_date.desc()).first()
        
        # Calculate Average Consumption for validation
        consumptions = BilConsumption.query.filter_by(meter_id=m.id).all()
        avg_cons = 0
        if consumptions:
            total_cons = sum(c.consumption for c in consumptions)
            avg_cons = total_cons / len(consumptions)
        
        # Check if there is already a consumption record for the current month
        c_this_month = BilConsumption.query.filter_by(meter_id=m.id, month=current_month).first()
        new_read = c_this_month.new_read if c_this_month else ''
        new_date = c_this_month.new_date.strftime("%Y-%m-%d") if c_this_month and c_this_month.new_date else current_date
        last_read = c_this_month.last_read if c_this_month else ''
        last_date = c_this_month.last_date.strftime("%Y-%m-%d") if c_this_month and c_this_month.last_date else ''

        meters_data.append({
            'meter': m,
            'meter_number': m.meter_number,
            'utility_type': m.utility_type,
            'pointing_to': m.pointing_to,
            'hierarchy': hierarchy,
            'last_reading': last_reading,
            'avg_cons': round(avg_cons, 2),
            'new_read': new_read,
            'new_date': new_date,
            'last_read': last_read,
            'last_date': last_date
        })
        
    return render_template("program_billing/input_readings.html", 
                           property=prop, 
                           meters_data=meters_data,
                           current_month=current_month,
                           current_date=current_date)

