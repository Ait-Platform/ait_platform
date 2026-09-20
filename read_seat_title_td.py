with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

start = text.find('<td class="p-4 align-middle">')
end = text.find('<!-- Actions -->')
print(text[start:start+1000])
