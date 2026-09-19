with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

route_code = """
@uip_bp.route("/<org_slug>/secretary/organogram", methods=["GET", "POST"])
@login_required
def secretary_organogram(org_slug):
    org = g.organization
    _require_secretary()
    
    from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
    
    # Pre-populate basic 4 Core ExCo seats if Blueprint is totally empty
    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 0:
        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3),
            ("Secretary", "CORE_EXCO", "Voluntary", 4)
        ]
        for title, grp, qual, order in default_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
            db.session.add(seat)
        db.session.commit()
    
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add_seat":
            seat = UipOrganogramSeat(
                organization_id=org.id,
                title=request.form.get("title"),
                group_level=request.form.get("group_level"),
                qualifier=request.form.get("qualifier"),
                display_order=99
            )
            db.session.add(seat)
            db.session.commit()
            flash(f"Blueprint seat '{seat.title}' added.", "success")
        elif action == "upload_photo":
            member_id = request.form.get("member_id")
            photo_url = request.form.get("photo_url")
            member = UipCommitteeMember.query.get(member_id)
            if member and member.organization_id == org.id:
                member.photo_url = photo_url
                db.session.commit()
                flash(f"Photo uploaded for {member.name}.", "success")
        return redirect(url_for("uip_bp.secretary_organogram", org_slug=org.slug))
    
    # 1. Fetch Blueprint Seats
    core_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="CORE_EXCO").order_by(UipOrganogramSeat.display_order).all()
    second_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="SECOND_GROUP").order_by(UipOrganogramSeat.id).all()
    
    # 2. Map active members to seats (For MVP: Map them by matching exact title since they don't have seat_ids properly linked in the DB from the genesis flow yet)
    active_members = UipCommitteeMember.query.filter_by(organization_id=org.id, status="CURRENT").all()
    
    # Attach members to seats temporarily for the view
    for seat in core_seats + second_seats:
        seat.member = None
        for m in active_members:
            if m.position.lower() == seat.title.lower():
                seat.member = m
                break
                
    return render_template("program_uip/dashboards/secretary_organogram.html", core_seats=core_seats, second_seats=second_seats)
"""

text += route_code

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added organogram route")
