with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('open_claims={"length": open_claims_count} # mock array length for the template', 'open_claims=[None] * open_claims_count')

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed mock array length")
