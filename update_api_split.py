import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the api search term extractor
regex = r'search_term = f"%\{reg_number\.strip\(\)\}%"'
new_logic = '''# Support datalist selections like "XYZ123 - Graham"
    actual_search = reg_number.split(" - ")[0].strip()
    search_term = f"%{actual_search}%"'''

content = re.sub(regex, new_logic, content)

# update clean_reg
content = content.replace('clean_reg = reg_number.strip().replace(" ", "")', 'clean_reg = actual_search.replace(" ", "")')

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
