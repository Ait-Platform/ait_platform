import re

print("==== SIMULATOR ====")
with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()
    scripts = re.findall(r'<script>(.*?)</script>', text, re.DOTALL)
    for s in scripts:
        print(s)

print("==== FACILITATOR ====")
with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()
    scripts = re.findall(r'<script>(.*?)</script>', text, re.DOTALL)
    for s in scripts:
        print(s)
