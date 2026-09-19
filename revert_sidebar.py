with open("templates/program_uip/base.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("{% if is_secretary or is_exco %}", "{% if is_secretary %}")

with open("templates/program_uip/base.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Reverted sidebar hide for ExCo")
