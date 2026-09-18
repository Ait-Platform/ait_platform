with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("pos = current_appointment.position.lower()", "pos = current_appointment.position.strip().lower() if current_appointment.position else \"\"")

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched dashboard pos logic")
