import os

with open('app/uip/routes.py', 'rb') as f:
    data = f.read()

# Remove BOM
if data.startswith(b'\xef\xbb\xbf'):
    data = data[3:]

# Remove null bytes
if b'\x00' in data:
    print("Found null bytes in routes.py! Fixing...")
    data = data.replace(b'\x00', b'')

with open('app/uip/routes.py', 'wb') as f:
    f.write(data)
