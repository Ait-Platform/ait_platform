import re
with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('<input type="hidden" name="position" value="Chairman"/>', '<input type="hidden" name="position" value="Chairperson"/>')
text = text.replace('<input type="hidden" name="position" value="Vice Chairman"/>', '<input type="hidden" name="position" value="Vice-Chairperson"/>')

with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated router values")
