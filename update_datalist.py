import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update datalist option format
regex = r'<option value="\{\{ v\.license_plate \}\}">\{\{ v\.client\.name \}\} \(\{\{ v\.make \}\} \{\{ v\.model \}\}\)</option>'
new_option = '<option value="{{ v.license_plate }} - {{ v.client.name }}">{{ v.make }} {{ v.model }}</option>'
content = re.sub(regex, new_option, content)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
