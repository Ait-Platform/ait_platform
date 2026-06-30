import re

with open('templates/program_budget/price.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace words
content = content.replace('BudgetCash', 'Mechanic CRM')
content = content.replace('budget_bp', 'mechanic_bp')
content = content.replace('budget', 'mechanic') # wait, this might be risky, but let's be careful

# Fix the specific link arguments:
# url_for('yoco_bp.yoco_start', email=current_user.email, subject='mechanic')
# url_for('auth_bp.register', role='user', subject='mechanic', plan='trial', next=url_for('auth_bp.bridge_dashboard'), price_id=price.price_id)
# This is handled automatically by replacing 'budget' with 'mechanic' inside url_for calls.

# Theme colors replacements:
content = content.replace('bg-amber-500', 'bg-indigo-600')
content = content.replace('bg-amber-600', 'bg-indigo-600')
content = content.replace('hover:bg-amber-700', 'hover:bg-indigo-700')
content = content.replace('text-amber-600', 'text-indigo-600')
content = content.replace('text-amber-700', 'text-indigo-700')
content = content.replace('text-amber-800', 'text-indigo-800')
content = content.replace('bg-amber-50', 'bg-indigo-50')
content = content.replace('border-amber-100', 'border-indigo-100')

# Features List for Mechanic CRM
features_old = '''              <li>Full Platform Access</li>
              <li>Unlimited Account Creation</li>
              <li>Statement Imports & Snapshots</li>
              <li>Automatic Balance Sheets</li>'''

features_new = '''              <li>Full Dashboard Access</li>
              <li>Unlimited Job Cards</li>
              <li>Client & Vehicle Management</li>
              <li>Invoicing System</li>'''

content = content.replace(features_old, features_new)

with open('templates/program_mechanic/price.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Created mechanic price.html!")
