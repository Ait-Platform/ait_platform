import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# Change H1
old_h1 = """            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-gavel text-indigo-700 mr-3"></i> Digital Committee Room
            </h1>"""
new_h1 = """            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>"""
text = text.replace(old_h1, new_h1)

# Change H2 inside document
old_h2 = '<h2 class="text-2xl font-black text-slate-900 mb-4">{{ resolution.title }}</h2>'
new_h2 = '<h2 class="text-lg font-bold text-slate-500 uppercase tracking-widest mb-4">Official Text</h2>'
text = text.replace(old_h2, new_h2)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated headers")
