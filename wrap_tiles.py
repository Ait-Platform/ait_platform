import re

with open('templates/public/welcome.html', 'r', encoding='utf-8') as f:
    content = f.read()

# The grid div contains multiple tiles. Let's find each tile.
# Tiles start with `  <!-- slug name -->` and end with `  </div>\n\n`
# We will use regex to wrap them.

slug_map = {
    'Loss': 'loss',
    'Reading': 'reading',
    'Home': 'home',
    'Tutor Registration': 'tutor',
    'Budget': 'budget',
    'Billing': 'billing',
    'Cultural Fire': 'cultural_fire',
    'Health Care Customer Relation Management': 'practice_crm',
    'Healthcare Data Switch': 'hds',
    'Receptionist Registration': 'receptionist',
    'SPV': 'spv',
    'Grade 12 Mathematics': 'adv_math',
    'Mechanic': 'mechanic'
}

for friendly_name, slug in slug_map.items():
    pattern = r'(  <!-- ' + friendly_name + r' -->\n  <div class="bg-white rounded-xl shadow overflow-hidden">.*?  </div>\n  </div>)'
    replacement = r'  {% if settings.get("visibility_' + slug + r'", "visible") != "hidden" %}\n\1\n  {% endif %}'
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)

with open('templates/public/welcome.html', 'w', encoding='utf-8') as f:
    f.write(content)
