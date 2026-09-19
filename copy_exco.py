with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("Secretary Dashboard", "Executive Dashboard")
text = text.replace("User Verification", "Verification Log") # Just a tweak for Chair view

with open("templates/program_uip/dashboards/exco_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Created exco_workspace.html")
