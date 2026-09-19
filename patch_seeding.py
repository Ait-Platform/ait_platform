with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_logic = """    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 0:
        default_seats = [
            ("Chairperson", "EXECUTIVE", "Mandatory", 1),
            ("Vice-Chairperson", "EXECUTIVE", "Voluntary", 2),
            ("Treasurer", "EXECUTIVE", "Mandatory", 3),
            ("Secretary", "EXECUTIVE", "Mandatory", 4)
        ]
        for title, grp, qual, order in default_seats:
            db.session.add(UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order))
        db.session.commit()"""

new_logic = """    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 0:
        default_seats = [
            ("Chairperson", "EXECUTIVE", "Mandatory", 1),
            ("Vice-Chairperson", "EXECUTIVE", "Voluntary", 2),
            ("Treasurer", "EXECUTIVE", "Mandatory", 3),
            ("Secretary", "EXECUTIVE", "Mandatory", 4),
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in default_seats:
            db.session.add(UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order))
        db.session.commit()"""

text = text.replace(old_logic, new_logic)
with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched routes.py seeding logic")
