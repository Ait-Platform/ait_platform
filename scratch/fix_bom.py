with open('app/uip/routes.py', 'rb') as f:
    data = f.read()

# Replace any BOM that is NOT at the start of the file
# Or just replace all \xef\xbb\xbf and add one at the beginning if needed
# Better: decode to string ignoring BOM, then encode
text = data.decode('utf-8-sig')
with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
