import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Add operations_seats to the route query
old_fetch = """    # 1. Fetch Blueprint Seats
    core_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="CORE_EXCO").order_by(UipOrganogramSeat.display_order).all()
    second_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="SECOND_GROUP").order_by(UipOrganogramSeat.id).all()"""

new_fetch = """    # 1. Fetch Blueprint Seats
    core_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="CORE_EXCO").order_by(UipOrganogramSeat.display_order).all()
    second_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="SECOND_GROUP").order_by(UipOrganogramSeat.id).all()
    operations_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="OPERATIONS").order_by(UipOrganogramSeat.id).all()"""

if old_fetch in text:
    text = text.replace(old_fetch, new_fetch)

# Update the loop that attaches members to seats
old_loop = "    for seat in core_seats + second_seats:"
new_loop = "    for seat in core_seats + second_seats + operations_seats:"
if old_loop in text:
    text = text.replace(old_loop, new_loop)

# Update render_template
old_render = 'return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats)'
new_render = 'return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats, operations_seats=operations_seats)'
if old_render in text:
    text = text.replace(old_render, new_render)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated route for operations seats")
