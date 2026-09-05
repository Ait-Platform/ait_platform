import re

file_path = 'templates/program_sace/post_test/results.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Add a Retake button next to "Back to Hub"
old_buttons = r'<a href="\{\{ url_for\(\'sace_bp.reading_hub\'\) \}\}" class="px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-semibold rounded-lg transition text-sm">\s*<i class="fas fa-arrow-left mr-1"></i> Back to Hub\s*</a>'

new_buttons = '''<div class="flex gap-2">
                <a href="{{ url_for('sace_bp.simulator') }}" class="px-4 py-2 bg-indigo-100 hover:bg-indigo-200 text-indigo-700 font-semibold rounded-lg transition text-sm">
                    <i class="fas fa-redo mr-1"></i> Retake Assessment
                </a>
                <a href="{{ url_for('sace_bp.reading_hub') }}" class="px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 font-semibold rounded-lg transition text-sm">
                    <i class="fas fa-arrow-left mr-1"></i> Back to Hub
                </a>
            </div>'''

text = re.sub(old_buttons, new_buttons, text)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
