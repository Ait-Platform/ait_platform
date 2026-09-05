import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = """
@sace_bp.route('/sace/hub')
@login_required
def selection_hub():
    return render_template('program_sace/sace_selection_hub.html')
"""

if "def selection_hub" not in content:
    content += new_route

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Added selection_hub route")
