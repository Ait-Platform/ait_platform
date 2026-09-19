with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("Live Tally & Quorum", "Live Tally & Participation")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html")
