filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('link.relationship = "owner"', 'link.relationship = "owner"\n                link.record_source = "MUNICIPAL"')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added record_source to link")
