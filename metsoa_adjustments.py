import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update build_electrical_rows
elec_old = '''def build_electrical_rows(property_id, month):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    linked_meter_ids = [m.id for m in meters]
    meter_types = {m.id: m.utility_type for m in BilMeter.query.filter(BilMeter.id.in_(linked_meter_ids)).all()}
    records = BilConsumption.query.filter_by(month=month).all()
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    rate_obj = BilTariff.query.filter_by(utility_type="electricity").first()
    rate = rate_obj.rate if rate_obj else 0

    for r in tenant_records:
        if (meter_types.get(r.meter_id) or "").lower() in ["electricity", "electrical"]:
            due = round(r.consumption * rate, 2)
            total_due += due
            rows.append({
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": r.days,
                "avg": 0, # Placeholder
                "consumption": r.consumption,
                "rate": rate,
                "due": due
            })
    return rows, round(total_due, 2)'''

elec_new = '''def build_electrical_rows(property_id, month):
    rows = []
    total_due = 0

    meters = get_all_property_meters(property_id)
    active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']
    meter_types = {m.id: m.utility_type for m in meters}
    
    # Pre-calculate sub-meter deductions for bulk meters
    records = BilConsumption.query.filter_by(month=month).all()
    bulk_deductions = {}
    for r in records:
        m = next((meter for meter in meters if meter.id == r.meter_id), None)
        if m and m.parent_meter_id:
            bulk_deductions[m.parent_meter_id] = bulk_deductions.get(m.parent_meter_id, 0) + r.consumption
            
    linked_meter_ids = [m.id for m in meters]
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    rate_obj = BilTariff.query.filter_by(utility_type="electricity").first()
    rate = rate_obj.rate if rate_obj else 0

    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
            
        if (meter_types.get(r.meter_id) or "").lower() in ["electricity", "electrical"]:
            adj_cons = r.consumption - bulk_deductions.get(r.meter_id, 0)
            adj_cons = max(0, adj_cons)
            
            due = round(adj_cons * rate, 2)
            total_due += due
            rows.append({
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": r.days,
                "avg": 0, # Placeholder
                "consumption": adj_cons,
                "rate": rate,
                "due": due
            })
    return rows, round(total_due, 2)'''

text = text.replace(elec_old, elec_new)


# 2. Update build_water_rows
water_old = '''def build_water_rows(property_id, month):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    linked_meter_ids = [m.id for m in meters]
    meter_types = {m.id: m.utility_type for m in BilMeter.query.filter(BilMeter.id.in_(linked_meter_ids)).all()}
    records = BilConsumption.query.filter_by(month=month).all()
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    for r in tenant_records:
        if (meter_types.get(r.meter_id) or "").lower() == "water":
            meter_id = r.meter_id
            
            summary = {
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": f"{r.days} KL/day",
                "avg": round(r.consumption / r.days, 1) if r.days else 0,
                "consumption": r.consumption,
                "rate": "",
                "due": ""
            }
            
            details = build_ws_sd_rows_for_meter(meter_id, month)
            if details:
                details["summary"] = summary
                water_meters.append(details)
                total_water_due += details["total"]

    return water_meters, round(total_water_due, 2)'''

water_new = '''def build_water_rows(property_id, month):
    water_meters = []
    total_water_due = 0

    meters = get_all_property_meters(property_id)
    active_meter_ids = [m.id for m in meters if (m.status or 'active').lower() == 'active']
    meter_types = {m.id: m.utility_type for m in meters}
    
    # Pre-calculate sub-meter deductions for bulk meters
    records = BilConsumption.query.filter_by(month=month).all()
    bulk_deductions = {}
    for r in records:
        m = next((meter for meter in meters if meter.id == r.meter_id), None)
        if m and m.parent_meter_id:
            bulk_deductions[m.parent_meter_id] = bulk_deductions.get(m.parent_meter_id, 0) + r.consumption
            
    linked_meter_ids = [m.id for m in meters]
    tenant_records = [r for r in records if r.meter_id in linked_meter_ids]

    for r in tenant_records:
        if r.meter_id not in active_meter_ids:
            continue
            
        if (meter_types.get(r.meter_id) or "").lower() == "water":
            meter_id = r.meter_id
            adj_cons = r.consumption - bulk_deductions.get(r.meter_id, 0)
            adj_cons = max(0, adj_cons)
            
            summary = {
                "meter_number": r.meter_number,
                "last_date": r.last_date.strftime('%Y/%m/%d'),
                "new_date": r.new_date.strftime('%Y/%m/%d'),
                "last_read": r.last_read,
                "new_read": r.new_read,
                "days": f"{r.days} KL/day",
                "avg": round(adj_cons / r.days, 1) if r.days else 0,
                "consumption": adj_cons,
                "rate": "",
                "due": ""
            }
            
            details = build_ws_sd_rows_for_meter(meter_id, month, adjusted_consumption=adj_cons)
            if details:
                details["summary"] = summary
                water_meters.append(details)
                total_water_due += details["total"]

    return water_meters, round(total_water_due, 2)'''

text = text.replace(water_old, water_new)

# 3. Update build_ws_sd_rows_for_meter
build_ws_old = '''def build_ws_sd_rows_for_meter(meter_id, month):
    # Step 1: Pull the meter object
    meter = BilMeter.query.get(meter_id)
    if not meter:
        return None  # Handle missing meter

    # Step 2: Pull consumption record using meter_id
    record = BilConsumption.query.filter_by(meter_id=meter_id, month=month).first()
    if not record:
        return None  # Handle missing data

    consumption = record.consumption
    days = record.days'''

build_ws_new = '''def build_ws_sd_rows_for_meter(meter_id, month, adjusted_consumption=None):
    # Step 1: Pull the meter object
    meter = BilMeter.query.get(meter_id)
    if not meter:
        return None  # Handle missing meter

    # Step 2: Pull consumption record using meter_id
    record = BilConsumption.query.filter_by(meter_id=meter_id, month=month).first()
    if not record:
        return None  # Handle missing data

    if adjusted_consumption is not None:
        consumption = adjusted_consumption
    else:
        consumption = record.consumption
    days = record.days'''

text = text.replace(build_ws_old, build_ws_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated build_electrical_rows, build_water_rows, and build_ws_sd_rows_for_meter for adjustments')
