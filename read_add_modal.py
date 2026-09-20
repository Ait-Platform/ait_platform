with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

start = text.find('id="addSeatModal"')
end = text.find('id="photoModal"')
print(text[start:end])
