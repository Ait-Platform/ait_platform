with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(">Organogram Builder<", ">Organogram & Member Assignment Builder<")

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated Organogram tile title")
