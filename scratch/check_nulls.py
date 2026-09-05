import os

for root, dirs, files in os.walk('app'):
    for f in files:
        if f.endswith('.py'):
            path = os.path.join(root, f)
            with open(path, 'rb') as file:
                data = file.read()
            if b'\x00' in data:
                print(f"Null bytes found in {path}! Fixing...")
                data = data.replace(b'\x00', b'')
                with open(path, 'wb') as file:
                    file.write(data)
print("Null byte check complete.")
