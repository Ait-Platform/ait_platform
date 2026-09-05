import re

file_path = 'templates/subject_reading/exit.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

sace_btn = '''
      <a href="{{ url_for('sace_bp.reading_hub') }}"
         class="w-full inline-flex items-center justify-between rounded-lg border-2 border-indigo-500 bg-indigo-50 px-4 py-2 hover:bg-indigo-100 font-bold text-indigo-900 mt-2 mb-2">
        <span>📖 Return to SACE Dashboard</span>
        <span class="text-xs text-indigo-600">Back to Auditor Hub</span>
      </a>
'''

if 'sace_bp.reading_hub' not in text:
    text = text.replace('<a href="{{ url_for(\'auth_bp.bridge_dashboard\') }}"', sace_btn + '\n      <a href="{{ url_for(\'auth_bp.bridge_dashboard\') }}"')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

