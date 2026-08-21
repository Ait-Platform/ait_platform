import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

route_original = '''        customer_name = request.form.get("customer_name")
        vehicle_reg = request.form.get("vehicle_reg")
        
        if not customer_name or not customer_name.strip():
            flash("Customer Name is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
            
        if not vehicle_reg or not vehicle_reg.strip():
            flash("Vehicle Registration is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
        # Process dynamic labor and parts arrays
        labor_descs = request.form.getlist('labor_desc[]')
        labor_ins = request.form.getlist('labor_in[]')
        labor_outs = request.form.getlist('labor_out[]')
        labor_rates = request.form.getlist('labor_rate[]')

        part_qtys = request.form.getlist('part_qty[]')
        part_descs = request.form.getlist('part_desc[]')
        part_rates = request.form.getlist('part_rate[]')
        import uuid
        
        # Mock finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()
        if not client:
            client = MechClient(name=customer_name)
            db.session.add(client)
            db.session.flush()
            
        vehicle = MechVehicle.query.filter_by(license_plate=vehicle_reg, client_id=client.id).first()
        if not vehicle:
            vehicle = MechVehicle(license_plate=vehicle_reg, make="Unknown", client_id=client.id)
            db.session.add(vehicle)
            db.session.flush()'''

route_new = '''        customer_name = request.form.get("customer_name")
        customer_phone = request.form.get("customer_phone")
        customer_email = request.form.get("customer_email")
        vehicle_reg = request.form.get("vehicle_reg")
        
        if not customer_name or not customer_name.strip():
            flash("Customer Name is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
            
        if not vehicle_reg or not vehicle_reg.strip():
            flash("Vehicle Registration is required.", "danger")
            return redirect(url_for('mechanic_bp.new_quote'))
            
        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
        engine_no = request.form.get("engine_no")
        gvm = request.form.get("gvm")
        tare = request.form.get("tare")
        disk_license_no = request.form.get("disk_license_no")
        
        # Process dynamic labor and parts arrays
        labor_descs = request.form.getlist('labor_desc[]')
        labor_ins = request.form.getlist('labor_in[]')
        labor_outs = request.form.getlist('labor_out[]')
        labor_rates = request.form.getlist('labor_rate[]')

        part_qtys = request.form.getlist('part_qty[]')
        part_descs = request.form.getlist('part_desc[]')
        part_rates = request.form.getlist('part_rate[]')
        import uuid
        
        # Finding or creating client
        client = MechClient.query.filter_by(name=customer_name).first()
        if not client:
            client = MechClient(name=customer_name, phone=customer_phone, email=customer_email)
            db.session.add(client)
            db.session.flush()
        else:
            if customer_phone and not client.phone:
                client.phone = customer_phone
            if customer_email and not client.email:
                client.email = customer_email
            
        vehicle = MechVehicle.query.filter_by(license_plate=vehicle_reg, client_id=client.id).first()
        if not vehicle:
            vehicle = MechVehicle(license_plate=vehicle_reg, make=make or "Unknown", client_id=client.id)
            db.session.add(vehicle)
            db.session.flush()
            
        # Update vehicle details if provided
        if vin_number: vehicle.vin = vin_number
        if make: vehicle.make = make
        if model: vehicle.model = model
        if year: vehicle.year = year
        if engine_no: vehicle.engine_no = engine_no
        if gvm: vehicle.gvm = gvm
        if tare: vehicle.tare = tare
        if disk_license_no: vehicle.disk_license_no = disk_license_no
        '''

content = content.replace(route_original, route_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated new_quote backend logic")
