with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

marker = """@uip_bp.route("/<org_slug>/resolution/<int:res_id>/edit", methods=["GET", "POST"])
@login_required
def edit_resolution(org_slug, res_id):"""

parts = text.split(marker)

# parts[1] is the duplicate block. Let's see if parts[2] starts with parts[1].
if parts[2].startswith(parts[1]):
    print("YES! parts[2] starts with exactly parts[1]")
    # So we can just remove parts[1] entirely (along with the first marker)!
    # Actually, parts[0] + marker + parts[2] will reconstruct the file perfectly without the duplicate.
    
    clean_text = parts[0] + marker + parts[2]
    with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
        f.write(clean_text)
    print("Fixed duplication!")
else:
    print("No, they don't match exactly. Let's inspect them.")
    print("Part 1 start:", repr(parts[1][:100]))
    print("Part 2 start:", repr(parts[2][:100]))

