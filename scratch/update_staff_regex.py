import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the Temporary password field block using regex
content = re.sub(r'<div>\s*<label[^>]*>Temporary Password</label>.*?</div>', '', content, flags=re.DOTALL)

# Update the instruction text using regex
content = re.sub(
    r'<strong>Create Receptionist Account:</strong>.*?</p>',
    '<strong>Link Receptionist Account:</strong> Add your receptionist by entering their email below. If they do not have an account yet, please use the WhatsApp invite feature at the bottom of this page to invite them to register first.</p>',
    content,
    flags=re.DOTALL
)

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Regex replacement done.")
