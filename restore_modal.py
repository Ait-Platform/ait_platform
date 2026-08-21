with open('dashboard_old.html', 'r', encoding='utf-16') as f:
    content = f.read()

import re
# Let's just find the entire div
modal_match2 = re.search(r'(<div id="manual-setup-modal".*?</div>\s*</div>\s*</div>\s*</div>)', content, re.DOTALL)
if modal_match2:
    modal_code = modal_match2.group(1)
    with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f2:
        new_content = f2.read()
        
    new_content = new_content.replace('{% endblock %}', modal_code + '\n{% endblock %}')
    
    with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f2:
        f2.write(new_content)
    print("Modal restored successfully (pattern 2).")
else:
    print("Could not find modal code.")
