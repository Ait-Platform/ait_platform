with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Update bulk meter creation
old_bulk = 'meter = BilMeter(meter_number=m_number, utility_type=u_type, pointing_to="Bulk Supply", municipal_bill_number=m_acc_number)'
new_bulk = 'meter = BilMeter(meter_number=m_number, utility_type=u_type, pointing_to="Bulk Supply", municipal_bill_number=m_acc_number, status=m_data.get("status", "active"))'

if old_bulk in content:
    content = content.replace(old_bulk, new_bulk)

# Update linked meter creation
old_linked = 'meter = BilMeter(meter_number=m_number, utility_type=u_type, parent_meter_id=parent_id, pointing_to="Sub-Unit", municipal_bill_number=m_acc_number)'
new_linked = 'meter = BilMeter(meter_number=m_number, utility_type=u_type, parent_meter_id=parent_id, pointing_to="Sub-Unit", municipal_bill_number=m_acc_number, status=m_data.get("status", "active"))'

if old_linked in content:
    content = content.replace(old_linked, new_linked)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py")
