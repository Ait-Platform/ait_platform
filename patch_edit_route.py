with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

new_logic = """        elif action == "edit_seat":
            seat_id = request.form.get("seat_id", type=int)
            seat = UipOrganogramSeat.query.get(seat_id)
            if seat and seat.organization_id == org.id:
                seat.title = request.form.get("title", seat.title)
                seat.group_level = request.form.get("group_level", seat.group_level)
                seat.qualifier = request.form.get("qualifier", seat.qualifier)
                seat.duty = request.form.get("duty", seat.duty)
                db.session.commit()
                flash(f"Blueprint seat '{seat.title}' updated.", "success")"""

start_idx = text.find('elif action == "assign_member":')
if start_idx != -1:
    text = text[:start_idx] + new_logic + "\n        " + text[start_idx:]
    
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added edit_seat handler")
