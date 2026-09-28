with open("templates/program_uip/dashboards/municipal_officer.html", "r", encoding="utf-8") as f:
    text = f.read()

old_btn = """<button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-file-upload mr-2"></i> Upload RP Vault</button>"""
new_btn = """<a href="{{ url_for('uip_bp.register_import', org_slug=org.slug) }}" class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center"><i class="fas fa-file-upload mr-2"></i> Upload RP Vault</a>"""

text = text.replace(old_btn, new_btn)

with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated UI upload button")
