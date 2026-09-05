import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the Link Receptionist block
content = re.sub(
    r'<strong>Link Receptionist Account:</strong>.*?</p>',
    '<strong>Create Receptionist Account:</strong> Add a new receptionist by entering their Name and Email below. The system will instantly create their account with the default password <strong>12345678</strong>. Once they log in, they can securely change this password on their My Account page.</p>',
    content,
    flags=re.DOTALL
)

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated staff.html")
