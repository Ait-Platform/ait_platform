import re

with open('templates/program_practice_crm/staff.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the instructional text
old_text = """          <div class="bg-blue-50 text-blue-900 text-sm p-4 rounded-lg mb-4 border border-blue-100 space-y-3">
            <p>
            <strong>Create Receptionist Account:</strong> You can create a fully registered account for your receptionist directly below. They will immediately be able to login to the platform using the email and password you set for them here.
            </p>
          </div>"""

new_text = """          <div class="bg-blue-50 text-blue-900 text-sm p-4 rounded-lg mb-4 border border-blue-100 space-y-3">
            <p>
            <strong>Link Receptionist Account:</strong> Add your receptionist by entering their email below. If they do not have an account yet, please use the WhatsApp invite feature at the bottom of this page to invite them to register first.
            </p>
          </div>"""

content = content.replace(old_text, new_text)

# 2. Remove the Temporary Password field
old_pw_field = """              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Temporary Password</label>
                <input type="password" name="password" autocomplete="new-password" placeholder="Leave blank if already registered" class="w-full rounded border-2 border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 px-4 py-2 bg-white">
              </div>"""

content = content.replace(old_pw_field, "")

with open('templates/program_practice_crm/staff.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated HTML")
