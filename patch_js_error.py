with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

broken_js = """ else {
        builder.classList.add('hidden');
        presentation.classList.remove('hidden');
    }
}"""
text = text.replace(broken_js, "")

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed broken JS")
