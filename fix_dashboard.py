with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

old_pattern = r'data = get_dashboard_data\(\)\s*return render_template\("program_billing/manager_dashboard\.html", data=data\)'

new_code = """data = get_dashboard_data()

    # Check for any in-progress draft property to enable the next tiles
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status.like('draft_%')
    ).first()

    return render_template("program_billing/manager_dashboard.html", data=data, draft_property=draft_property)"""

if re.search(old_pattern, content):
    content = re.sub(old_pattern, new_code, content)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('Successfully updated!')
else:
    print('Pattern not found!')
