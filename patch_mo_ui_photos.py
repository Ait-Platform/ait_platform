with open("templates/program_uip/dashboards/municipal_officer.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("{% if referral.interaction.documents %}", "{% if referral.photos %}")
text = text.replace("{% for doc in referral.interaction.documents %}", "{% for doc in referral.photos %}")

with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated UI template to use referral.photos")
