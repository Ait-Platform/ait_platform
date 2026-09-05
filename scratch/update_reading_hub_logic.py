import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix the completed calculation logic to remove reviewer_guide and change total to 6
# Note: There are two sets of this block in the file due to earlier template injections!
# I will replace them globally via regex

pattern_calc = r'\{% set total = 7 %\}.*?\{% set percent = \(completed / total \* 100\) \| round \| int %\}'
replacement_calc = '''{% set total = 6 %}
            {% set completed = 0 %}
            {% if progress.app_form %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.patent %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.annexures %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.ppp %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.demo_cert %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.reading_cert %}{% set completed = completed + 1 %}{% endif %}
            {% set percent = (completed / total * 100) | round | int %}'''
text = re.sub(pattern_calc, replacement_calc, text, flags=re.DOTALL)

# Remove the duplicated/messy assignments that might have been injected earlier
# Wait, let's just make sure we strip any rogue reviewer_guide increments
text = re.sub(r'\{% if progress\.reviewer_guide %\}.*?\{% endif %\}\n?', '', text)

# Remove Reviewer Guide row from roadmap
pattern_rg = r'<!-- Reviewer Guide -->.*?</div>\s*</div>\s*</div>'
# Wait, a simple string replace is safer if I know the exact HTML
