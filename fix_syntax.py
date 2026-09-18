with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the founding additions
text = text.replace("                additions += f\"- {claim.creator.name} ({claim.creator.email}) as {role_name}\n\"", "                additions += f\"- {claim.creator.name} ({claim.creator.email}) as {role_name}\\n\"")

# Fix the new res description
text = text.replace("        res.description += f\"\n- {claim.creator.name}: {role_name}\"", "        res.description += f\"\\n- {claim.creator.name}: {role_name}\"")

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
