with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """    # Calculate occupied singular seats so the UI can grey them out
    occupied = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT",
        func.lower(UipCommitteeMember.position).in_(["chairperson", "chairman", "chair", "vice-chairperson", "vice chairman", "vice chair", "secretary", "treasurer"])
    ).all()
    
    # Normalize the output for the template
    occupied_seats = []
    for m in occupied:
        pos = m.position.lower()
        if pos in ["chairman", "chair"]: pos = "chairperson"
        if pos in ["vice chairman", "vice chair"]: pos = "vice-chairperson"
        occupied_seats.append(pos)
    
    return render_template("program_uip/router.html", org=org, occupied_seats=occupied_seats)"""

new_logic = """    # --- DYNAMIC ORGANOGRAM SEATS SEEDING ---
    from app.models.uip_governance import UipOrganogramSeat
    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 0:
        default_seats = [
            ("Chairperson", "EXECUTIVE", "Mandatory", 1),
            ("Vice-Chairperson", "EXECUTIVE", "Voluntary", 2),
            ("Treasurer", "EXECUTIVE", "Mandatory", 3),
            ("Secretary", "EXECUTIVE", "Mandatory", 4)
        ]
        for title, grp, qual, order in default_seats:
            db.session.add(UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order))
        db.session.commit()
        
    exco_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="EXECUTIVE").order_by(UipOrganogramSeat.display_order).all()
    sub_seats = UipOrganogramSeat.query.filter_by(organization_id=org.id, group_level="SECOND_GROUP").order_by(UipOrganogramSeat.display_order).all()
    # ----------------------------------------

    # Calculate ALL occupied seats so the UI can grey them out
    occupied = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.status == "CURRENT"
    ).all()
    
    # Normalize the output for the template
    occupied_seats = []
    for m in occupied:
        pos = m.position.lower().strip()
        if pos in ["chairman", "chair"]: pos = "chairperson"
        if pos in ["vice chairman", "vice chair"]: pos = "vice-chairperson"
        occupied_seats.append(pos)
    
    return render_template("program_uip/router.html", org=org, occupied_seats=occupied_seats, exco_seats=exco_seats, sub_seats=sub_seats)"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched routes.py")
