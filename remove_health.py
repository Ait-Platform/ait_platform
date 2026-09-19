import re
with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()

# Strip out Precinct Activation Health completely
pattern = re.compile(r'<!-- PRECINCT ACTIVATION HEALTH -->.*?<!-- PENDING ACCESS CLAIMS -->', re.DOTALL)
if pattern.search(text):
    text = pattern.sub('<!-- PENDING ACCESS CLAIMS -->', text)
else:
    # try just removing the block if the PENDING ACCESS CLAIMS marker is missing
    alt_pattern = re.compile(r'<!-- PRECINCT ACTIVATION HEALTH -->.*?(?=<header class="ui-card-head">)', re.DOTALL)
    text = alt_pattern.sub('', text)

with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed Activation Health")
