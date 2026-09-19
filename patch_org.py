with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    'return render_template("program_uip/dashboards/secretary_organogram.html", core_seats=core_seats, second_seats=second_seats)',
    'return render_template("program_uip/dashboards/secretary_organogram.html", org=org, core_seats=core_seats, second_seats=second_seats)'
)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added org to organogram template render")
