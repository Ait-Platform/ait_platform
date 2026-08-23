import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'Payment strictly within 30 days.',
    "1. THANK YOU FOR YOUR SUPPORT!\n2. All work is completed to a high standard using genuine parts.\n3. Parts remain the property of the business until paid in full.\n4. Vehicles are stored and driven at owner\\'s risk."
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
