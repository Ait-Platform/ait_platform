import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Replace the specific block
search_regex = r"data\['date'\] = inv\.timestamp\.strftime\(\"%Y-%m-%d\"\)\s+auditors\.append\(data\)"
replace_str = "data['date'] = inv.timestamp.strftime(\"%Y-%m-%d\")\n            data['id'] = inv.id\n            auditors.append(data)"

routes = re.sub(search_regex, replace_str, routes)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
