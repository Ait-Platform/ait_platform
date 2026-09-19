with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("Back to Command Switchboard", "Back to Secretary Dashboard")
text = text.replace("Resolution Register", "Secretary Dashboard")

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee.html text")
