with open('app/models/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('UipResolution\\n', 'UipResolution, UipDocument, UipBroadcast\\n')

with open('app/models/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
