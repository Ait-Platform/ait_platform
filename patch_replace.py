with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace all occurrences of replace(''', '\'') with replace("'", "\\'")
# But wait, in python string, ''' is hard to match. Let's just use regex.
import re
text = re.sub(r"replace\(''', '\\''\)", "replace(\"'\", \"\\\\'\")", text)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed replace syntax")
