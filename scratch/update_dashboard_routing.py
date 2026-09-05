import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_dash = """    elif role_slug == "committee_member":
        return render_template("uip/dashboards/committee.html", org=org)
"""

text = text.replace('    elif role_slug == "manager":', new_dash + '    elif role_slug == "manager":')

with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
