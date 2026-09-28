import re

filepath = 'templates/program_uip/navigation.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''<a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" style="display: flex; align-items: center; padding: 0.75rem 1rem; font-weight: 700; color: #4338ca; background: #e0e7ff; border-radius: 0.5rem; margin-bottom: 1rem; text-decoration: none; transition: background 0.2s;" onmouseover="this.style.background='#c7d2fe'" onmouseout="this.style.background='#e0e7ff'">
    <i class="fas fa-th-large mr-3 text-indigo-600"></i> Secretary Control
</a>'''

replace_code = '''<a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" style="display: flex; align-items: center; padding: 0.75rem 1rem; font-weight: 700; color: #4338ca; background: #e0e7ff; border-radius: 0.5rem; margin-bottom: 1rem; text-decoration: none; transition: background 0.2s;" onmouseover="this.style.background='#c7d2fe'" onmouseout="this.style.background='#e0e7ff'">
    <i class="fas fa-th-large mr-3 text-indigo-600"></i> Committee Board
</a>'''

content = content.replace(search_code, replace_code)
with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
