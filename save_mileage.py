import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add mileage to form processing
old_form = '''        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
        # Process dynamic labor and parts arrays'''

new_form = '''        vin_number = request.form.get("vin_number")
        make = request.form.get("make")
        model = request.form.get("model")
        year_str = request.form.get("year")
        year = int(year_str) if year_str and year_str.isdigit() else None
        mileage = request.form.get("mileage")
        # Process dynamic labor and parts arrays'''

content = content.replace(old_form, new_form)

# Add mileage to job card creation
old_job = '''        job_card = MechJobCard(
            shop_id=active_shop.id,
            vehicle_id=vehicle.id,
            job_number=job_number,
            status='Quote',
            total_amount=0.0
        )
        db.session.add(job_card)'''

new_job = '''        job_card = MechJobCard(
            shop_id=active_shop.id,
            vehicle_id=vehicle.id,
            job_number=job_number,
            status='Quote',
            total_amount=0.0,
            mileage=mileage
        )
        db.session.add(job_card)'''

content = content.replace(old_job, new_job)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
