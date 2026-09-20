with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('CoreRole.query.filter_by(slug="resident").first()',
                    'CoreRole.query.filter_by(slug="owner").first() or CoreRole.query.filter_by(slug="resident").first()')

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated role fallback in secretary_routes.py")
