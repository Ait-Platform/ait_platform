import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_text = "Once they log in, they can securely change this password on their My Account page."
new_text = "The receptionist must enter via the Login link found on the top bar of the app. Once they log in, they can securely change this password on their My Account page."

content = content.replace(old_text, new_text)

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated CRM instructions")
