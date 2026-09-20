with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_seed1 = """        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3),
            ("Secretary", "CORE_EXCO", "Voluntary", 4),
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in default_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)"""

new_seed1 = """        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1, "manager"),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2, "manager"),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3, "manager"),
            ("Secretary", "CORE_EXCO", "Voluntary", 4, "manager"),
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5, "committee_member"),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6, "committee_member"),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7, "committee_member"),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8, "committee_member"),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9, "committee_member")
        ]
        for title, grp, qual, order, duty in default_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order, duty=duty)"""

old_seed2 = """        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]
        for title, grp, qual, order in new_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order)"""

new_seed2 = """        new_seats = [
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5, "committee_member"),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6, "committee_member"),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7, "committee_member"),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8, "committee_member"),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9, "committee_member")
        ]
        for title, grp, qual, order, duty in new_seats:
            seat = UipOrganogramSeat(organization_id=org.id, title=title, group_level=grp, qualifier=qual, display_order=order, duty=duty)"""

text = text.replace(old_seed1, new_seed1)
text = text.replace(old_seed2, new_seed2)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated seeding logic in secretary_routes.py")
