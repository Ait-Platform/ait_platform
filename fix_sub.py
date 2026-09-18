import re

with open("templates/program_uip/claim_committee.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('<input type="hidden" name="position" value="Subcommittee Member"/>\n        ', '')

with open("templates/program_uip/claim_committee.html", "w", encoding="utf-8") as f:
    f.write(text)
