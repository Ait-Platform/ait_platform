import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Remove the /sace/post_ws_survey route
survey_route = re.search(r'(@sace_bp\.route\("/sace/post_ws_survey".*?return render_template\("program_sace/post_ws_survey\.html"\)\n)', text, re.DOTALL)
if survey_route:
    text = text.replace(survey_route.group(1), '')

# Remove survey from progress dictionary in reading_hub
text = text.replace("'survey': 'post_ws_survey' in completed_slugs", "")
text = text.replace(",\n        \n    }", "\n    }")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)

file_path_hub = 'templates/program_sace/reading_hub.html'
with open(file_path_hub, 'r', encoding='utf-8') as f: text_hub = f.read()

# Remove survey block from hub
survey_block = re.search(r'(<!-- Phase 5: Post-Workshop Survey -->.*?</div>\s*</div>\s*</div>)', text_hub, re.DOTALL)
if survey_block:
    text_hub = text_hub.replace(survey_block.group(1), '')

# Adjust total from 8 to 7
text_hub = text_hub.replace('{% set total = 8 %}', '{% set total = 7 %}')
text_hub = text_hub.replace('{% if progress.survey %}{% set completed = completed + 1 %}{% endif %}', '')

with open(file_path_hub, 'w', encoding='utf-8') as f: f.write(text_hub)

