with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i in range(80, min(100, len(lines))):
    print(f"{i}: {lines[i].strip()}")
