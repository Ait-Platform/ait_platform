import codecs

routes_path = 'app/auth/routes.py'
with open(routes_path, 'rb') as f:
    content = f.read()

if content.startswith(codecs.BOM_UTF8):
    content = content[len(codecs.BOM_UTF8):]

with open(routes_path, 'wb') as f:
    f.write(content)
