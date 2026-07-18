import re
with open('templates/program_billing/property_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()
    for line in text.split('\n'):
        if 'hidden' in line and 'prev' in line:
            print(line.strip())
