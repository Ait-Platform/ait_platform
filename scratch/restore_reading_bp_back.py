import re

file_path_1 = 'templates/subject_reading/learner_dashboard.html'
with open(file_path_1, 'r', encoding='utf-8') as f: text1 = f.read()

pattern1 = r'\{% set is_sace = namespace\(value=false\) %\}.*?\{% endif %\}'
replacement1 = '''<a href="{{ url_for('auth_bp.bridge_dashboard', force=1) }}" class="inline-flex items-center rounded-lg px-4 py-2 text-sm font-semibold border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 transition">
              &larr; Back
            </a>'''
text1 = re.sub(pattern1, replacement1, text1, flags=re.DOTALL)
with open(file_path_1, 'w', encoding='utf-8') as f: f.write(text1)


file_path_2 = 'templates/subject_reading/exit.html'
with open(file_path_2, 'r', encoding='utf-8') as f: text2 = f.read()
pattern2 = r'<a href="\{\{ url_for\(\'sace_bp\.reading_hub\'\) \}\}".*?</a>'
text2 = re.sub(pattern2, '', text2, flags=re.DOTALL)
with open(file_path_2, 'w', encoding='utf-8') as f: f.write(text2)

