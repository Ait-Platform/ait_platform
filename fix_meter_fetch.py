import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

helper = '''
def get_all_property_meters(property_id):
    from app.models.billing import BilProperty, BilSectionalUnit, BilMuniAccount, BilMeter
    units = BilSectionalUnit.query.filter_by(property_id=property_id).all()
    all_meters = []
    for u in units:
        all_meters.extend(u.meters)
        
    muni_accounts = BilMuniAccount.query.filter_by(property_id=property_id).all()
    muni_acc_numbers = [acc.account_number for acc in muni_accounts if acc.account_number]
    if muni_acc_numbers:
        muni_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(muni_acc_numbers)).all()
        for m in muni_meters:
            if m not in all_meters:
                all_meters.append(m)
                
    for acc in muni_accounts:
        if acc.water_meter and acc.water_meter not in all_meters:
            all_meters.append(acc.water_meter)
        if acc.elec_meter and acc.elec_meter not in all_meters:
            all_meters.append(acc.elec_meter)
            
    return all_meters

'''

elec_old = '''def build_electrical_rows(property_id, month):
    from app.models.billing import BilSectionalUnit
    rows = []
    total_due = 0

    unit_ids = [u.id for u in BilSectionalUnit.query.filter_by(property_id=property_id).all()]
    linked_meter_ids = [m.id for m in BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()]'''

elec_new = '''def build_electrical_rows(property_id, month):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    linked_meter_ids = [m.id for m in meters]'''

water_old = '''def build_water_rows(property_id, month):
    from app.models.billing import BilSectionalUnit
    water_meters = []
    total_water_due = 0

    unit_ids = [u.id for u in BilSectionalUnit.query.filter_by(property_id=property_id).all()]
    linked_meter_ids = [m.id for m in BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()]'''

water_new = '''def build_water_rows(property_id, month):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    linked_meter_ids = [m.id for m in meters]'''

# Insert helper before build_electrical_rows
text = text.replace('def build_electrical_rows', helper.strip() + '\n\ndef build_electrical_rows', 1)
text = text.replace(elec_old, elec_new)
text = text.replace(water_old, water_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated builders to use comprehensive property meter fetch logic')
