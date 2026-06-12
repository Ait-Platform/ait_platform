import os

files = [
    'app/subject_loss/routes.py',
    'app/subject_loss/services.py'
]

for fpath in files:
    with open(fpath, 'r', encoding='utf-8') as f:
        content = f.read()
    content = content.replace("datetime('now')", "CURRENT_TIMESTAMP")
    with open(fpath, 'w', encoding='utf-8') as f:
        f.write(content)
print("Done!")
