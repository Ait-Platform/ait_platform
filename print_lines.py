with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for j in range(430, 460):
    print(f"  {j}: {lines[j].strip()}")
