with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """        if action == "add_seat":
            seat = UipOrganogramSeat(
                organization_id=org.id,
                title=request.form.get("title"),
                group_level=request.form.get("group_level"),
                qualifier=request.form.get("qualifier"),
                display_order=99
            )"""

new_logic = """        if action == "add_seat":
            seat = UipOrganogramSeat(
                organization_id=org.id,
                title=request.form.get("title"),
                group_level=request.form.get("group_level"),
                qualifier=request.form.get("qualifier"),
                duty=request.form.get("duty", "committee_member"),
                display_order=99
            )"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated add_seat in routes")
