with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

marker = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):"""

parts = text.split(marker)

# Find the common prefix
import os
common = os.path.commonprefix([parts[1], parts[2]])
print(f"Common prefix length: {len(common)} out of {len(parts[1])}")

# Let's print the divergent part
print("Part 1 at divergence:", repr(parts[1][len(common):len(common)+100]))
print("Part 2 at divergence:", repr(parts[2][len(common):len(common)+100]))
