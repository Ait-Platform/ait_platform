import re
with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()
    dashboards = re.findall(r'@cultural_bp\.route\("([^"]+dashboard[^"]*)"', text)
    print("Dashboards:", dashboards)
