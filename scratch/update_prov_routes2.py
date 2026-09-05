import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Make sure we add inv.id to data
old_list = '''      auditors = []
      for inv in invites:
          try:
              data = json.loads(inv.response_data)
              data['date'] = inv.timestamp.strftime("%Y-%m-%d")
              auditors.append(data)
          except Exception:
              pass'''
              
new_list = '''      auditors = []
      for inv in invites:
          try:
              data = json.loads(inv.response_data)
              data['date'] = inv.timestamp.strftime("%Y-%m-%d")
              data['id'] = inv.id
              auditors.append(data)
          except Exception:
              pass'''

routes = routes.replace(old_list, new_list)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
