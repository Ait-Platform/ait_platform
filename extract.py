with open('scratch_old_routes.py', 'r', encoding='utf-16') as f:
    lines = f.readlines()

start_idx = -1
end_idx = -1
for i, line in enumerate(lines):
    if '@mechanic_bp.route("/mechanic/quote/new", methods=["GET", "POST"])' in line:
        start_idx = i
    if 'def messages():' in line:
        end_idx = i - 2 # assuming some decorators exist
        break

new_quote_str = "".join(lines[start_idx:end_idx])

# Now modify new_quote_str to include the VIN + VAT logic

# 1. Capture fields
new_capture = '''
        customer_name = request.form.get("customer_name")
        vehicle_reg = request.form.get("vehicle_reg")
        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
'''
new_quote_str = new_quote_str.replace('        customer_name = request.form.get("customer_name")\n        vehicle_reg = request.form.get("vehicle_reg")', new_capture.strip('\n'))

# 2. File upload logic
file_upload_logic = '''
        import os
        from werkzeug.utils import secure_filename
        
        license_disk_image = request.files.get("license_disk_image")
        filename = None
        if license_disk_image and license_disk_image.filename:
            upload_folder = os.path.join(current_app.root_path, "static", "uploads", "mechanic")
            os.makedirs(upload_folder, exist_ok=True)
            filename = secure_filename(license_disk_image.filename)
            import time
            filename = f"{int(time.time())}_{filename}"
            license_disk_image.save(os.path.join(upload_folder, filename))
            
        if vin_number: vehicle.vin = vin_number
        if make: vehicle.make = make
        if model: vehicle.model = model
        if year: vehicle.year = year
        if filename: vehicle.license_disk_url = filename
'''
new_quote_str = new_quote_str.replace('        if not vehicle:\n            vehicle = MechVehicle(license_plate=vehicle_reg, make="Unknown", client_id=client.id)\n            db.session.add(vehicle)\n            db.session.flush()', '        if not vehicle:\n            vehicle = MechVehicle(license_plate=vehicle_reg, make="Unknown", client_id=client.id)\n            db.session.add(vehicle)\n            db.session.flush()\n' + file_upload_logic)

# 3. VAT logic
vat_logic = '''
        job_card = MechJobCard(
            job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",
            vehicle_id=vehicle.id,
            status='Quote',
            vat_rate=active_shop.vat_rate
        )
'''
new_quote_str = new_quote_str.replace('        job_card = MechJobCard(\n            job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",\n            vehicle_id=vehicle.id,\n            status=\'Ready\'\n        )', vat_logic.strip('\n'))

with open('app/program_mechanic/routes.py', 'a', encoding='utf-8') as f:
    f.write('\n\n')
    f.write(new_quote_str)
