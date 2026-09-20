with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

start = text.find('{% if seat.qualifier %}')
end = text.find('<!-- Actions -->')
print(text[start:end])
