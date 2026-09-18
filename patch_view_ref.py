import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_h1 = """            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>"""

new_h1 = """            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>"""

text = text.replace(old_h1, new_h1)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html with reference")
