with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# We know the modal starts with <!-- Manual Setup Modal --> and ends with {% endblock %}
# Actually, the string <div id="manual-setup-modal" appears twice. Let's find both.

matches = [m.start() for m in re.finditer(r'<div id="manual-setup-modal"', content)]

if len(matches) > 1:
    # We want to remove the FIRST instance. Let's find where it starts and ends.
    start_idx = matches[0]
    # To find the end, it's safer to use regex to find the start of the SECOND instance.
    end_idx = matches[1]
    
    # We'll just slice the string and remove everything between start_idx and end_idx
    # Wait, there might be other code between them!
    # Let's inspect the exact lines where manual-setup-modal is.
    print(f"Modal 1 at: {start_idx}, Modal 2 at: {end_idx}")
else:
    print(f"Only {len(matches)} found.")
