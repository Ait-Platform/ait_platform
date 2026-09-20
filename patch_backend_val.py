with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Add duplicate check logic to add_seat
old_add_seat = """        if action == "add_seat":
            seat = UipOrganogramSeat(
                organization_id=org.id,
                title=request.form.get("title"),
                group_level=request.form.get("group_level"),
                qualifier=request.form.get("qualifier"),
                duty=request.form.get("duty", "committee_member"),
                display_order=99
            )
            db.session.add(seat)
            db.session.commit()
            flash(f"Blueprint seat '{seat.title}' added.", "success")"""

new_add_seat = """        if action == "add_seat":
            title = request.form.get("title")
            # Check for duplicate
            existing = UipOrganogramSeat.query.filter(
                UipOrganogramSeat.organization_id == org.id,
                db.func.lower(UipOrganogramSeat.title) == db.func.lower(title)
            ).first()
            if existing:
                flash(f"Warning: A seat with the title '{title}' already exists.", "danger")
            else:
                seat = UipOrganogramSeat(
                    organization_id=org.id,
                    title=title,
                    group_level=request.form.get("group_level"),
                    qualifier=request.form.get("qualifier"),
                    duty=request.form.get("duty", "committee_member"),
                    display_order=99
                )
                db.session.add(seat)
                db.session.commit()
                flash(f"Blueprint seat '{seat.title}' added.", "success")"""

text = text.replace(old_add_seat, new_add_seat)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added anti-duplication to add_seat")
