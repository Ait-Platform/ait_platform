import re

with open("templates/program_uip/dashboards/process_claims.html", "r", encoding="utf-8") as f:
    text = f.read()

# Add the claim.title and claim.description to the UI
old_ui = """                    <div>
                        <div class="font-bold text-slate-900">{{ claim.creator.name or "User" }}</div>
                        <div class="text-sm text-slate-500">{{ claim.creator.email }}</div>
                    </div>"""

new_ui = """                    <div>
                        <div class="font-bold text-slate-900">{{ claim.creator.name or "User" }}</div>
                        <div class="text-sm text-slate-500">{{ claim.creator.email }}</div>
                        <div class="mt-2 text-sm text-indigo-700 font-semibold">{{ claim.title }}</div>
                        <div class="text-xs text-slate-600">{{ claim.description }}</div>
                    </div>"""

text = text.replace(old_ui, new_ui)

with open("templates/program_uip/dashboards/process_claims.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated process_claims.html")

