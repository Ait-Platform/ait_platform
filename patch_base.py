with open("templates/program_uip/base.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("{% if is_secretary %}", "{% if is_secretary or is_exco %}")

with open("templates/program_uip/base.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated base.html to hide sidebar for all ExCo")
