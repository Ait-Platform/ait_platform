with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Let's count how many modals there are
import re
modals = re.findall(r'<div id="manual-setup-modal"', content)
print(f"Found {len(modals)} modals")
