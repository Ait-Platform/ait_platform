import re
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Let's remove the portfolio_map generation
text = re.sub(r'    # Process portfolio assignments.*?if target == "founding":', '    from datetime import datetime\n    current_year = datetime.now().year\n    \n    if target == "founding":', text, flags=re.DOTALL)

# Let's replace the `port = portfolio_map.get` logic
text = re.sub(r'port = portfolio_map\.get.*?additions \+= f"- \{claim\.creator\.name\} \(\{claim\.creator\.email\}\) as \{port\}\\n"', 'role_name = claim.interaction_type.replace(\'_claim\', \'\').title()\n                additions += f"- {claim.creator.name} ({claim.creator.email}) as {role_name}\\n"', text, flags=re.DOTALL)

# Let's replace position=port with position="Unassigned"
text = text.replace('position=port,', 'position="Unassigned",')

# Let's replace the portfolio logic in the "new" branch
text = re.sub(r'result_basis=\{"type": "access_bundle", "interaction_ids": \[c\.id for c in claims\], "portfolios": portfolio_map\}', 'result_basis={"type": "access_bundle", "interaction_ids": [c.id for c in claims]}', text, flags=re.DOTALL)

text = re.sub(r'port = portfolio_map\.get.*?res\.description \+= f"\\n- \{claim\.creator\.name\}: \{port\}"', 'role_name = claim.interaction_type.replace(\'_claim\', \'\').title()\n        res.description += f"\\n- {claim.creator.name}: {role_name}"', text, flags=re.DOTALL)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Regex replaced portfolio logic!")
