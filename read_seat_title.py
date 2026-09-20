with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

start = text.find('<th class="p-4 w-1/4">Seat Title</th>')
end = text.find('<!-- Actions -->')
print(text[start:start+1000])
