import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Change Title
text = text.replace('{% block title %}SACE Activity Evaluation Hub{% endblock %}', '{% block title %}Provider Litre Reading Activity{% endblock %}')
text = text.replace('<h1 class="text-3xl font-black mb-2">SACE Activity Evaluation Hub</h1>', '<h1 class="text-3xl font-black mb-2">Provider Litre Reading Activity</h1>')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
