import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace H1 title
text = text.replace('<h1 class="text-3xl font-extrabold text-slate-900 mt-1">Committee Dashboard</h1>',
                    '<h1 class="text-3xl font-extrabold text-slate-900 mt-1">{% if current_appointment and current_appointment.position == "Secretary" %}Secretary{% else %}Committee{% endif %} Dashboard</h1>')
text = text.replace('{% block title %}Committee Dashboard', '{% block title %}{% if current_appointment and current_appointment.position == "Secretary" %}Secretary{% else %}Committee{% endif %} Dashboard')

# Now fix the tiles. 
# Look for <div class="w-10 h-10 bg-indigo-600 text-white rounded-full flex items-center justify-center mx-auto mb-3 shadow-sm">
# and similar blocks and strip them out to make it compact.
text = re.sub(r'<div class="w-10 h-10[^>]+>.*?</div>', '', text, flags=re.DOTALL)
text = text.replace('class="ui-card p-5 text-center', 'class="ui-card p-3 text-center')

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated dashboard title and removed heavy circle icons")
