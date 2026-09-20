with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We need to replace the imports in public_organogram
old_imports = 'from app.models.uip_organogram import UipOrganization, UipBlueprintSeat'
new_imports = 'from app.models.uip import UipOrganization\n    from app.models.uip_governance import UipOrganogramSeat as UipBlueprintSeat\n    from app.models.uip_governance import UipCommitteeMember'
text = text.replace(old_imports, new_imports)

# Also need to make sure we map members to seats exactly like we do in secretary_routes
# Let's completely replace the public_organogram function body
pattern = r'@uip_bp\.route\("/<org_slug>/organogram"\)\ndef public_organogram\(org_slug\):.*?return render_template\(\s*"program_uip/dashboards/public_organogram\.html".*?\)'

new_function = """@uip_bp.route("/<org_slug>/organogram")
def public_organogram(org_slug):
    \"\"\"Public-facing visual organogram for membership drives\"\"\"
    from app.models.uip import UipOrganization
    from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
    
    org = UipOrganization.query.filter_by(slug=org_slug).first_or_404()
    
    core_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level='CORE_EXCO').order_by(UipOrganogramSeat.id).all()
    second_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level='SECOND_GROUP').order_by(UipOrganogramSeat.id).all()
    operations_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level='OPERATIONS').order_by(UipOrganogramSeat.id).all()

    # Map members to seats for display
    active_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
    for seat in core_seats + second_seats + operations_seats:
        seat.member = None
        for m in active_members:
            if m.position and m.position.lower() == seat.title.lower():
                seat.member = m
                break

    return render_template(
        "program_uip/dashboards/public_organogram.html",
        org=org,
        core_seats=core_seats,
        second_seats=second_seats,
        operations_seats=operations_seats
    )"""

text = re.sub(pattern, new_function, text, flags=re.DOTALL)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed imports and member mapping in public_organogram")
