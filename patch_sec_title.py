with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("Secretary Command", "Secretary Dashboard")

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Renamed Secretary Command to Secretary Dashboard")
