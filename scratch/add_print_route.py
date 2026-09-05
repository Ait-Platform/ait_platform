import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

print_route = '''
@sace_bp.route("/sace/provisioning/print_slip/<code>")
@login_required
def print_access_slip(code):
    return render_template("program_sace/print_slip.html", code=code)
'''
text += print_route

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
