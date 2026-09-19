with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

bad_block = """    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 4:
        # Hotfix: Add missing 5 subcommittees if they only have the first 4
        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in new_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
            db.session.add(seat)
        db.session.commit()
"""

# 1. Strip the bad block out
if bad_block in text:
    text = text.replace("\n\n" + bad_block, "")
    print("Stripped bad block")

# 2. Inject it properly in secretary_organogram
target = """        db.session.commit()
    
    if request.method == "POST":"""

injection = """        db.session.commit()
        
    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 4:
        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in new_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)
            db.session.add(seat)
        db.session.commit()
    
    if request.method == "POST":"""

if target in text:
    text = text.replace(target, injection)
    print("Injected good block")
else:
    print("Could not find target in secretary_organogram")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
