with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

migration_logic = """    if UipOrganogramSeat.query.filter_by(organization_id=org.id).count() == 4:
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

# inject right after the first populate block
if "db.session.commit()" in text:
    text = text.replace("db.session.commit()\n", "db.session.commit()\n\n" + migration_logic, 1)
    with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Added hotfix migration to secretary_routes.py")
