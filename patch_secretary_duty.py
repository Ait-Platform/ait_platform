with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# Add secretary to the duty dropdown in both the Add and Edit modals
old_options = """                    <option value="owner">Owner / Chairperson (Full Admin)</option>
                    <option value="manager">Manager / Vice-Chair (Full Access)</option>"""
new_options = """                    <option value="owner">Owner / Chairperson (Full Admin)</option>
                    <option value="manager">Manager / Vice-Chair (Full Access)</option>
                    <option value="secretary">Secretary (Admin & Records)</option>"""

text = text.replace(old_options, new_options)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Added secretary to duty lists")
