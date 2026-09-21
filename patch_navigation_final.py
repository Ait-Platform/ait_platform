with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Rename "Resolutions" to "Secretary Control"
text = text.replace("<summary>Resolutions</summary>", "<summary>Secretary Control</summary>")

# 2. Remove the Chairman Tools and Treasurer Tools blocks completely
pattern = r'{% if is_chairman %}.*?{% elif is_secretary %}'
text = re.sub(pattern, '{% if is_secretary %}', text, flags=re.DOTALL)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated navigation.html per user instructions")
