import os

with open('app/uip/__init__.py', 'rb') as f:
    data = f.read()

# Remove BOM
if data.startswith(b'\xef\xbb\xbf'):
    data = data[3:]

# Remove null bytes
data = data.replace(b'\x00', b'')

with open('app/uip/__init__.py', 'wb') as f:
    f.write(data)
