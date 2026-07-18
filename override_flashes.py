import os

templates = [
    'utilities_hub.html', 
    'soa_dashboard.html', 
    'soa_map.html', 
    'soa_tenants.html', 
    'soa_generate.html'
]

for t in templates:
    path = f'templates/program_billing/{t}'
    if not os.path.exists(path):
        continue
        
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
        
    if '{% block flashes %}' not in text:
        # Add it right after the block title
        old_title = '{% block title %}'
        end_title = '{% endblock %}'
        
        idx1 = text.find(old_title)
        if idx1 != -1:
            idx2 = text.find(end_title, idx1)
            if idx2 != -1:
                insert_pos = idx2 + len(end_title)
                
                new_text = text[:insert_pos] + '\n{% block flashes %}{% endblock %}' + text[insert_pos:]
                
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(new_text)

print('Added block flashes override to templates.')
