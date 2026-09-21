with open("templates/program_uip/my_access.html", "r", encoding="utf-8") as f:
    html = f.read()

old_header = """<div class="max-w-3xl mx-auto px-4 py-8">
    <div class="mb-10 border-b border-slate-200 pb-6">
        <h1 class="text-3xl font-extrabold text-slate-900">Access Request Status</h1>
        <p class="text-slate-500 mt-2 font-medium">View the status of your role claims and registration.</p>
    </div>"""

new_header = """<div class="max-w-3xl mx-auto px-4 py-8">
    <div class="mb-10 border-b border-slate-200 pb-6 flex justify-between items-end">
        <div>
            <h1 class="text-3xl font-extrabold text-slate-900">Access Request Status</h1>
            <p class="text-slate-500 mt-2 font-medium">View the status of your role claims and registration.</p>
        </div>
        <div class="text-right">
            <p class="text-indigo-600 font-bold text-lg mb-1">{{ current_user.name }}</p>
            <p class="text-xs text-slate-400 font-bold uppercase tracking-widest">{{ current_user.email }}</p>
        </div>
    </div>"""

html = html.replace(old_header, new_header)

with open("templates/program_uip/my_access.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated header for my_access")
