with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Let's find the exact string that is duplicated.
# It starts at:
# @uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
# @login_required
# def edit_resolution(org_slug, res_id):

marker = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):"""

parts = text.split(marker)
print(f"Found {len(parts)} parts")
# parts[0] is everything before the first edit_resolution
# parts[1] is everything between first edit_resolution and second edit_resolution
# parts[2] is everything after the second edit_resolution

if len(parts) == 3:
    # If parts[1] and parts[2] start similarly, they are duplicates.
    # We should just take parts[0] + marker + parts[2] (assuming parts[2] contains the rest of the file and isn't truncated)
    # Wait, let's see where the duplicate block ends.
    # The duplicate block probably ends at the exact same place parts[1] ends.
    # Let's check the lengths.
    print(f"Len part 1: {len(parts[1])}")
    print(f"Len part 2: {len(parts[2])}")
