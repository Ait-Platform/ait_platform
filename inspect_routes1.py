with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

marker = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):"""

parts = text.split(marker)
import re

routes = re.findall(r'@uip_bp\.route.*?def [a-zA-Z0-9_]+', parts[1], re.DOTALL)
for r in routes:
    print(r.replace('\n', ' '))
