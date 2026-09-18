import re
with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("<p class=\"ui-eyebrow\">Access Request</p>", "<p class=\"ui-eyebrow\">Official Registration</p>")
text = text.replace("<h1>Welcome to {{ org.name }}</h1>", "<h1>{{ org.name }} Member Registration</h1>")

with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated router wording")
