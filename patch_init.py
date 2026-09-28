import re
with open("app/program_uip/__init__.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('"uip_bp.register_import", ', '')

with open("app/program_uip/__init__.py", "w", encoding="utf-8") as f:
    f.write(text)
