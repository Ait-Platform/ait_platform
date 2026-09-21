with open("templates/program_uip/dashboards/chairman_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the name from the right side
text = text.replace('<div class="text-sm font-bold text-slate-700">{{ current_user.name }}</div>', '')

# Add the name to the left side
left_side = """<h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Chairman Dashboard</h1>
            <p class="text-slate-500 mt-2 font-medium">Alert-Driven Operations Dashboard</p>
            <p class="text-indigo-600 mt-1 font-bold text-sm">{{ current_user.name }}</p>"""

text = text.replace("""<h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Chairman Dashboard</h1>
            <p class="text-slate-500 mt-2 font-medium">Alert-Driven Operations Dashboard</p>""", left_side)

with open("templates/program_uip/dashboards/chairman_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)

# Also apply this to Treasurer
with open("templates/program_uip/dashboards/treasurer_workspace.html", "r", encoding="utf-8") as f:
    t_text = f.read()

t_text = t_text.replace('<div class="text-sm font-bold text-slate-700">{{ current_user.name }}</div>', '')
t_left_side = """<h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Treasurer Dashboard</h1>
            <p class="text-slate-500 mt-2 font-medium">Alert-Driven Operations Dashboard</p>
            <p class="text-indigo-600 mt-1 font-bold text-sm">{{ current_user.name }}</p>"""
t_text = t_text.replace("""<h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Treasurer Dashboard</h1>
            <p class="text-slate-500 mt-2 font-medium">Alert-Driven Operations Dashboard</p>""", t_left_side)

with open("templates/program_uip/dashboards/treasurer_workspace.html", "w", encoding="utf-8") as f:
    f.write(t_text)

print("Moved user names to the left")
