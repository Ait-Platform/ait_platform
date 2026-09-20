with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('resident_role = CoreRole.query.filter_by(slug="resident").first()', 
                    'resident_role = CoreRole.query.filter_by(slug="owner").first() or CoreRole.query.filter_by(slug="resident").first()')
text = text.replace('role_obj = CoreRole.query.filter_by(slug="resident").first()',
                    'role_obj = CoreRole.query.filter_by(slug="owner").first() or CoreRole.query.filter_by(slug="resident").first()')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated role fallback to owner/resident")
