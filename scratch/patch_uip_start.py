import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_route = """@uip_bp.route("/")
def uip_start():
    # A simple landing/entry route that redirects to the demo tenant for now.
    # We will assume 'manor-gardens' since we seeded it earlier.
    return redirect(url_for('uip_bp.dashboard', org_slug='manor-gardens'))"""

new_route = """@uip_bp.route("/")
def uip_start():
    # Public marketing and landing page for UIPs
    return render_template('uip/public_about.html')"""

text = text.replace(old_route, new_route)

with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
