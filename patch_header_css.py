for file in ["templates/program_uip/dashboards/secretary_organogram.html", "templates/program_uip/dashboards/secretary_intake.html", "templates/program_uip/dashboards/committee.html"]:
    with open(file, "r", encoding="utf-8") as f:
        text = f.read()
    
    text = text.replace('<header class="ui-page-head mb-8 border-b border-slate-200 pb-6">', '<header class="mb-8 border-b border-slate-200 pb-6 w-full">')
    
    with open(file, "w", encoding="utf-8") as f:
        f.write(text)
print("Removed ui-page-head class")
