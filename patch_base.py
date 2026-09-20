with open("templates/program_uip/base.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Remove the aggressive sidebar hiding logic based on is_secretary globally
pattern = r'{% if is_secretary %}\s*<style>\s*\.ui-sidebar \{ display: none !important; \}.*?</style>\s*{% endif %}'
text = re.sub(pattern, '', text, flags=re.DOTALL)

with open("templates/program_uip/base.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Removed global sidebar hiding from base.html")
