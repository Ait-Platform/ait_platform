import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''    auditors = []
    for ix in interactions:
        try:
            data = json.loads(ix.response_data)
            auditors.append({
                "id": ix.id,
                "first_name": data.get("first_name", ""),
                "last_name": data.get("last_name", ""),
                "email": data.get("email", ""),
                "status": data.get("status", "Invite Sent"),
                "date": ix.timestamp.strftime("%Y-%m-%d"),
                "code": data.get("code", "")
            })
        except:
            pass'''

new_logic = '''    auditors = []
    for ix in interactions:
        try:
            data = json.loads(ix.response_data)
            # Filter out legacy auditors that don't have a code
            if not data.get("code"):
                continue
                
            auditors.append({
                "id": ix.id,
                "first_name": data.get("first_name", ""),
                "last_name": data.get("last_name", ""),
                "email": data.get("email", ""),
                "status": data.get("status", "Invite Sent"),
                "date": ix.timestamp.strftime("%Y-%m-%d"),
                "code": data.get("code", "")
            })
        except:
            pass'''

text = text.replace(old_logic, new_logic)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
