with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Capture fields
old_target = """    target = request.form.get("resolution_target", "new")"""
new_target = """    target = request.form.get("resolution_target", "new")
    term_start = request.form.get("term_start_date", "")
    term_duration = request.form.get("term_duration_months", "12")"""
text = text.replace(old_target, new_target)

# Add to descriptions
old_additions = """            additions = "\\n\\n-- Added via Inaugural Roster --\\n\""""
new_additions = """            additions = f"\\n\\n-- Added via Inaugural Roster (Term: {term_start} for {term_duration} months) --\\n\""""
text = text.replace(old_additions, new_additions)

old_new_desc = """        res.description += f"\\n- {claim.creator.name}: {role_name}\""""
new_new_desc = """        res.description += f"\\n- {claim.creator.name}: {role_name} (Term: {term_start} for {term_duration} months)\""""
text = text.replace(old_new_desc, new_new_desc)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
