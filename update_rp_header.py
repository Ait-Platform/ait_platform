import re

filepath = 'templates/program_uip/dashboards/ratepayer_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_header = '''<header class="ui-page-head">
    <div class="flex justify-between items-center mb-2">
        <h1 class="m-0">Owner Dashboard</h1>
        <a class="ui-btn" href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}">
            <i class="fas fa-arrow-left mr-2"></i> Back
        </a>
    </div>
    <div class="flex justify-between items-end">
        <p class="ui-subtitle text-indigo-700 font-bold text-lg m-0">{{ member.name }}</p>
        <nav class="flex gap-3">
            <a href="#my-property" class="px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded text-sm font-bold shadow-sm transition">My Property</a>
            <a href="{{ url_for('uip_bp.public_mandates', org_slug=org.slug) }}" class="px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded text-sm font-bold shadow-sm transition">Mandates</a>
            <a href="#lodge-query" class="px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded text-sm font-bold shadow-sm transition">Lodge a Query</a>
            <a href="#my-queries" class="px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded text-sm font-bold shadow-sm transition">My Queries</a>
        </nav>
    </div>
</header>'''

# Replace header
content = re.sub(r'<header class="ui-page-head.*?</header>', new_header, content, flags=re.DOTALL)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated ratepayer header layout")
