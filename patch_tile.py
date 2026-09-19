with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace Gatekeeper with User Verification
if '<h2 class="text-xl font-bold text-slate-800 mb-2">Gatekeeper</h2>' in text:
    text = text.replace('<h2 class="text-xl font-bold text-slate-800 mb-2">Gatekeeper</h2>', '<h2 class="text-xl font-bold text-slate-800 mb-2">User Verification</h2>')
if 'Gatekeeper' in text:
    text = text.replace('Gatekeeper', 'User Verification')

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Renamed Tile 1")
