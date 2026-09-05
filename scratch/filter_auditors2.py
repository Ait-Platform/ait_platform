import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''    auditors = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            data['date'] = inv.timestamp.strftime("%Y-%m-%d")
            data['id'] = inv.id
            auditors.append(data)
        except Exception:
            pass'''

new_logic = '''    auditors = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            # Hide old legacy test accounts that don't have access codes
            if not data.get("code"):
                continue
                
            data['date'] = inv.timestamp.strftime("%Y-%m-%d")
            data['id'] = inv.id
            auditors.append(data)
        except Exception:
            pass'''

text = text.replace(old_logic, new_logic)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
