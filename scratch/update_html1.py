import re

# 1. Update reading_hub.html
with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    hub = f.read()

# Title
hub = hub.replace("Sace Authorised User(s) Map", "Sace Auditor Map")

# Move IP Pledge (Patent Docs) to the top!
# It's currently a <tr> in the tbody. We need to cut it and paste it as the very first <tr>.
# Actually, the user says "start with pledge first what happens if they dont check this"
# I can make it the first row, and add 'required' to the checkbox.
pattern_patent = r'(<tr[^>]*>.*?<form action="{{ url_for\(\'sace_bp\.acknowledge_patent\'\).*?</tr>)'
match = re.search(pattern_patent, hub, re.DOTALL)
if match:
    patent_row = match.group(1)
    hub = hub.replace(patent_row, '') # remove from current position
    # Insert at the beginning of tbody
    tbody_start = hub.find('<tbody>') + len('<tbody>\n')
    # add required to the checkbox
    patent_row = patent_row.replace('type="checkbox"', 'type="checkbox" required')
    hub = hub[:tbody_start] + patent_row + hub[tbody_start:]

# Change "Provider Interactive Demo" -> "Workshop Interactive Demo"
hub = hub.replace("Provider Interactive Demo", "Workshop Interactive Demo")

# Bigger text for "Classroom Application" and "evaluation"
hub = hub.replace('<div class="font-bold text-slate-800">Classroom Application</div>', '<div class="font-black text-lg text-slate-800">Classroom Application</div>')
hub = hub.replace('<div class="font-bold text-slate-800">SACE Post-Test & Evaluation</div>', '<div class="font-black text-lg text-slate-800">SACE Post-Test & Evaluation</div>')

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(hub)

# 2. Update presentation_ppp.html
with open('templates/program_sace/presentation_ppp.html', 'r', encoding='utf-8') as f:
    ppp = f.read()

ppp = ppp.replace("SACE Evaluation Presentation", "Litre Reading Presentation")

# Replace evaluator note
old_note = "Evaluator Note: This is a simplified linear presentation (PPP) of the program's methodology and flow. During live sessions, participants interact dynamically via their mobile devices in a synchronized &quot;Seesaw&quot; format with the facilitator's projector."
new_note = "Evaluator Note: This is a simplified linear presentation (PPP) of the program's methodology and flow."
ppp = ppp.replace(old_note, new_note)
# Make it slightly bigger
ppp = ppp.replace('text-xs text-purple-200 mt-2', 'text-sm font-medium text-purple-100 mt-2')

with open('templates/program_sace/presentation_ppp.html', 'w', encoding='utf-8') as f:
    f.write(ppp)

