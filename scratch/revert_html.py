import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the text block
old_text = """          <div class="bg-blue-50 text-blue-900 text-sm p-4 rounded-lg mb-4 border border-blue-100 space-y-3">
            <p>
            <strong>Link Receptionist Account:</strong> Add your receptionist by entering their email below. If they do not have an account yet, please use the WhatsApp invite feature at the bottom of this page to invite them to register first.</p>
          </div>"""

new_text = """          <div class="bg-blue-50 text-blue-900 text-sm p-4 rounded-lg mb-4 border border-blue-100 space-y-3">
            <p>
            <strong>Create Receptionist Account:</strong> Add a new receptionist by entering their Name and Email below. The system will instantly create their account with the default password <strong>12345678</strong>. Once they log in, they can securely change this password on their My Account page.</p>
          </div>"""

content = content.replace(old_text, new_text)

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
