import re

with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_default = """        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3),
            ("Secretary", "CORE_EXCO", "Voluntary", 4)
        ]"""

new_default = """        default_seats = [
            ("Chairperson", "CORE_EXCO", "Voluntary", 1),
            ("Vice-Chairperson", "CORE_EXCO", "Voluntary", 2),
            ("Treasurer", "CORE_EXCO", "Voluntary", 3),
            ("Secretary", "CORE_EXCO", "Voluntary", 4),
            ("Security Sub-Committee Lead", "SECOND_GROUP", "Voluntary", 5),
            ("Greening & Environment Lead", "SECOND_GROUP", "Voluntary", 6),
            ("Infrastructure & Maintenance Lead", "SECOND_GROUP", "Voluntary", 7),
            ("Social & Community Lead", "SECOND_GROUP", "Voluntary", 8),
            ("Finance & Audit Lead", "SECOND_GROUP", "Voluntary", 9)
        ]"""

if old_default in text:
    text = text.replace(old_default, new_default)
    with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched secretary_routes.py with default seats")
else:
    print("Could not find old_default")
