with open("old_organogram.html", "r", encoding="utf-16") as f:
    text = f.read()

start = text.find('<div id="presentationMode"')
end = text.find('<!-- MODALS -->')
print(text[start:end])
