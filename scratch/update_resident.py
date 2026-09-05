import re

with open('templates/uip/dashboards/resident.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('<button class="px-6 py-2 bg-indigo-600', '<a href="{{ url_for(\'uip_bp.new_interaction\', org_slug=org.slug) }}" class="px-6 py-2 bg-indigo-600')
text = text.replace('</button>\n    </div>', '</a>\n    </div>')

text = text.replace('<div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm flex justify-between items-center">', '<a href="{{ url_for(\'uip_bp.view_interaction\', org_slug=org.slug, reference=ix.reference) }}" class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm flex justify-between items-center hover:shadow-md transition block">')

# Close the anchor tag correctly
text = text.replace('</span>\n                    </div>', '</span>\n                    </a>')

with open('templates/uip/dashboards/resident.html', 'w', encoding='utf-8') as f:
    f.write(text)
