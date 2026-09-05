import os
path = 'app/models/uip.py'
with open(path, 'rb') as f:
    data = f.read()

if data.startswith(b'\xef\xbb\xbf'):
    data = data[3:]
data = data.replace(b'\x00', b'')

with open(path, 'wb') as f:
    f.write(data)
