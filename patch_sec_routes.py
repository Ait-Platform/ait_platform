import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    content = f.read()

# Add logic for action == "add_subcommittee"
search_block = """        elif action == "upload_photo":
            member_id = request.form.get("member_id")"""
replace_block = """        elif action == "add_subcommittee":
            from app.program_uip.services import subcommittees as sub_service
            try:
                sub_service.create_subcommittee(
                    org.id, current_user.id,
                    request.form.get("name"),
                    request.form.get("resolution_id", type=int),
                    request.form.get("responsible_seat_id", type=int),
                    request.form.get("reports_to_seat_id", type=int)
                )
                db.session.commit()
                flash("Subcommittee registered successfully.", "success")
            except Exception as e:
                db.session.rollback()
                flash(str(e.description if hasattr(e, "description") else e), "danger")
        elif action == "upload_photo":
            member_id = request.form.get("member_id")"""

content = content.replace(search_block, replace_block)

# Add resolutions and subcommittees to the render context
context_search = """    # Attach members to seats temporarily for the view
    for seat in core_seats + second_seats + operations_seats:
        seat.member = None
        for m in active_members:
            if m.position.lower() == seat.title.lower():
                seat.member = m
                break
                
    return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats, operations_seats=operations_seats, active_members=active_members)"""

context_replace = """    # Attach members to seats temporarily for the view
    for seat in core_seats + second_seats + operations_seats:
        seat.member = None
        for m in active_members:
            if m.position.lower() == seat.title.lower():
                seat.member = m
                break
                
    from app.models.uip import UipResolution
    from app.program_uip.services import subcommittees as sub_service
    adopted_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").order_by(UipResolution.id.desc()).all()
    subcommittees = sub_service.get_subcommittees(org.id)
    for sub in subcommittees:
        sub.responsible_member = sub_service.resolve_responsible_member(sub)
                
    return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats, operations_seats=operations_seats, active_members=active_members, adopted_resolutions=adopted_resolutions, subcommittees=subcommittees)"""

content = content.replace(context_search, context_replace)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Updated secretary_routes.py")
