import re
with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("{% if 'chairman' in occupied_seats %}", "{% if 'chairperson' in occupied_seats %}")
text = text.replace("{% if 'vice chairman' in occupied_seats or 'vice chair' in occupied_seats %}", "{% if 'vice-chairperson' in occupied_seats %}")

with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated router.html template conditions")
