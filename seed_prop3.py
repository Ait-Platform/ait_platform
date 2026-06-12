from app import create_app
from app.extensions import db
from app.models.billing import BilProperty, BilSectionalUnit, BilMeter, BilConsumption, BilTenant
from datetime import datetime

app = create_app()

data = [
    # type, meter, old_date, new_date, old_read, new_read, days, cons
    ("electricity", "7150696S", "2026/04/30", "2026/05/29", 49546, 49553, 29, 7),
    ("electricity", "9027800S", "2026/04/30", "2026/05/29", 83361, 83412, 29, 51),
    ("electricity", "9027600S", "2026/04/30", "2026/05/29", 25687, 25946, 29, 259),
    ("electricity", "796387S", "2026/03/31", "2026/04/30", 54865, 55253, 30, 388),
    ("electricity", "2188802S", "2026/04/30", "2026/05/29", 28303, 28699, 29, 396),
    ("electricity", "9343803S", "2026/04/30", "2026/05/29", 91462, 91978, 29, 516),
    ("water", "BLZ080", "2026/04/30", "2026/05/29", 2828, 2889, 29, 61),
    ("water", "AGN489", "2026/04/30", "2026/05/29", 14836, 14889, 29, 5.3),
    ("water", "CXN998", "2026/04/30", "2026/05/29", 597, 610, 29, 13),
    ("water", "CWR826", "2026/04/30", "2026/05/29", 213, 224, 29, 11),
    ("water", "CWR820", "2026/04/30", "2026/05/29", 158, 167, 29, 9),
    ("water", "CWA388", "2026/04/30", "2026/05/29", 279, 290, 29, 11),
    ("water", "CWA074", "2026/04/30", "2026/05/29", 251, 267, 29, 16),
]

with app.app_context():
    p = BilProperty.query.filter_by(name="prop3").first()
    if not p:
        print("prop3 not found")
        exit()

    unit = BilSectionalUnit.query.filter_by(property_id=p.id).first()
    if not unit:
        unit = BilSectionalUnit(property_id=p.id, unit_number="Main")
        db.session.add(unit)
        db.session.commit()
        
    tenant = BilTenant.query.filter_by(sectional_unit_id=unit.id).first()
    if not tenant:
        tenant = BilTenant(sectional_unit_id=unit.id, name="Graham Curtis Clack", email="grahamclack68@gmail.com")
        db.session.add(tenant)
        db.session.commit()

    for row in data:
        utype, mnum, old_d, new_d, old_r, new_r, days, cons = row
        meter = BilMeter.query.filter_by(meter_number=mnum, sectional_unit_id=unit.id).first()
        if not meter:
            meter = BilMeter(meter_number=mnum, utility_type=utype, sectional_unit_id=unit.id)
            db.session.add(meter)
            db.session.commit()

        # check if consumption exists
        c = BilConsumption.query.filter_by(meter_id=meter.id, month="2026-05").first()
        if not c:
            c = BilConsumption(meter_id=meter.id, month="2026-05")
            db.session.add(c)
        
        c.meter_number = mnum
        c.last_date = datetime.strptime(old_d, "%Y/%m/%d").date()
        c.new_date = datetime.strptime(new_d, "%Y/%m/%d").date()
        c.last_read = old_r
        c.new_read = new_r
        c.days = days
        c.consumption = cons
        
    db.session.commit()
    print("Seeded successfully!")
