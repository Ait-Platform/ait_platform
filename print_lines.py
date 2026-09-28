with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i in range(760, 770):
    print(f"{i+1}: {lines[i].strip()}")
