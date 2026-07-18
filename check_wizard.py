import re
with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()
    wizards = re.findall(r'@cultural_bp\.route\("([^"]+wizard[^"]*)"', text)
    print("Wizards:", wizards)
    biodata = re.findall(r'@cultural_bp\.route\("([^"]+bio[^"]*)"', text)
    print("Biodata:", biodata)
