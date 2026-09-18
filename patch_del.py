with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Remove the faulty lines 89-91
del lines[89:92]

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.writelines(lines)
