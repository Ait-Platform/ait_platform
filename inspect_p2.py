with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

marker = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):"""

parts = text.split(marker)

# Let's see what comes after the old vote_resolution in parts[2]
# The old vote_resolution ends with:
# return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))
end_marker = 'return redirect(url_for("uip_bp.view_resolution", org_slug=org.slug, res_id=res.id))'

subparts = parts[2].split(end_marker, 1)
print(f"Found end_marker in parts[2]: {len(subparts) == 2}")
if len(subparts) == 2:
    print("Next route starts with:", repr(subparts[1][:200]))
