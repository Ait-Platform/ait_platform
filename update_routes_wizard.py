import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the manual_capture route to render setup_wizard.html
old_render = "return render_template(\"program_billing/manual_capture.html\", property=draft_property, accounts=accounts, bulk_meters=bulk_meters)"
new_render = "return render_template(\"program_billing/setup_wizard.html\", property=draft_property, accounts=accounts, bulk_meters=bulk_meters)"
content = content.replace(old_render, new_render)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated manual_capture route.")
