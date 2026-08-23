import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the Completed tile button
btn_regex = r'<button onclick="switchTab\(\'completed\'\)".*?<span class="mb-1">Completed</span>.*?<span class="bg-slate-300 text-slate-700 py-0\.5 px-2 rounded-full font-black">\{\{ completed\|length \}\}</span>.*?</button>'
content = re.sub(btn_regex, '', content, flags=re.DOTALL)

# 2. Remove the Completed tab pane
pane_regex = r'<div id="tab-content-completed" class="tab-pane hidden">.*?\{\{ job_table\("Completed", completed, "green", "table-completed"\) \}.*?</div>'
# Wait, the macro call was: {{ job_table("Completed / Billed", completed, "green", "table-completed") }}
# Wait! In the clean restore, it might just be "Completed / Billed"
pane_regex_real = r'<div id="tab-content-completed" class="tab-pane hidden">.*?</div>'
# I need to be careful with this regex.
