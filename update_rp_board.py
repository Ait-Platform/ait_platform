filepath = 'templates/program_uip/dashboards/ratepayer_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_header = '''<header class="ui-page-head flex justify-between items-start">
    <div>
        <p class="ui-eyebrow">Municipal Vault</p>
        <h1>Owner Dashboard</h1>
        <p class="ui-subtitle text-indigo-700 font-bold text-lg mt-1">{{ member.name }}</p>
    </div>
    <div class="text-right flex flex-col items-end">
        <a class="ui-btn inline-flex items-center mb-4" href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}">
            <i class="fas fa-arrow-left mr-2"></i> Back
        </a>
        <nav class="flex gap-4 text-sm font-bold text-slate-500">
            <a href="#my-property" class="hover:text-indigo-600 transition">My Property</a>
            <a href="{{ url_for('uip_bp.public_mandates', org_slug=org.slug) }}" class="hover:text-indigo-600 transition">Mandates</a>
            <a href="#lodge-query" class="hover:text-indigo-600 transition">Lodge a Query</a>
            <a href="#my-queries" class="hover:text-indigo-600 transition">My Queries</a>
        </nav>
    </div>
</header>
{% include "partials/flash_messages.html" %}'''

import re
# Replace header and the old nav
content = re.sub(r'<header class="ui-page-head">.*?</nav>', new_header, content, flags=re.DOTALL)

# Remove the read-only text and the member name line from the property section
content = content.replace('<p>Read-only information from the Municipal Oversight Vault.</p>\n<p>{{ member.name }} ? {{ member.reference }}</p>', '')

# Replace title input with dropdown
old_input = '<input id="title" name="title" required maxlength="255" class="border border-slate-300 rounded p-2 w-full">'
new_select = '''<select id="title" name="title" required class="border border-slate-300 rounded p-2 w-full bg-white">
    <option value="" disabled selected>Select a category...</option>
    <option value="Water/Pipe Leak">Water/Pipe Leak</option>
    <option value="Pothole/Road Damage">Pothole/Road Damage</option>
    <option value="Streetlight Fault">Streetlight Fault</option>
    <option value="Illegal Dumping">Illegal Dumping</option>
    <option value="Overgrown Vegetation">Overgrown Vegetation</option>
    <option value="Security Incident">Security Incident</option>
    <option value="Other">Other (Please specify in description)</option>
</select>'''
content = content.replace(old_input, new_select)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated ratepayer dashboard UI")
