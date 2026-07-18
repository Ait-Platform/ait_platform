import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the manual_capture route to pass all meters
old_render = "return render_template(\"program_billing/setup_wizard.html\", property=draft_property, accounts=accounts, bulk_meters=bulk_meters)"

new_render = """    
    # Fetch all meters associated with these accounts
    account_numbers = [a.account_number for a in accounts if a.account_number]
    all_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(account_numbers)).all() if account_numbers else []
    
    return render_template("program_billing/setup_wizard.html", 
        property=draft_property, 
        accounts=accounts, 
        all_meters=all_meters)
"""
if "all_meters=all_meters" not in content:
    content = content.replace(old_render, new_render)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated manual_capture route to pass all_meters.")
