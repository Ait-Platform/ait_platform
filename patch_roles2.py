with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old1 = """                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or ("Secretary" if "secretary" in claim.interaction_type else claim.interaction_type.replace('_claim', '').title())"""
new1 = """                requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.interaction_type.replace('_claim', '').title()
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or ("Secretary" if "secretary" in claim.interaction_type else requested_pos)"""

old2 = """                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or claim.interaction_type.replace('_claim', '').title()"""
new2 = """                requested_pos = claim.title.split(": ")[-1] if ":" in claim.title else claim.interaction_type.replace('_claim', '').title()
                port = portfolio_map.get(str(claim.id)) or portfolio_map.get(claim.id) or requested_pos"""

text = text.replace(old1, new1)
text = text.replace(old2, new2)

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched committee_routes.py")
