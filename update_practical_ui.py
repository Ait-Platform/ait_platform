import glob
import re

for filepath in glob.glob('templates/subject_home/chapter*_practical.html'):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Replace the dynamic grid
    content = content.replace('{% if is_teacher_scoring %}grid md:grid-cols-2 gap-6{% else %}space-y-4{% endif %}', 'grid md:grid-cols-2 gap-6')

    # 2. Remove the if is_teacher_scoring wrappers around the right-hand checkbox divs.
    # We look for the {% if is_teacher_scoring %} followed by <div class="bg-slate-50 p-5 rounded-lg border border-slate-200 flex flex-col justify-center">
    pattern = r'\{%\s*if\s*is_teacher_scoring\s*%\}\s*(<div class="bg-slate-50 p-5 rounded-lg border border-slate-200 flex flex-col justify-center">.*?)\s*\{%\s*endif\s*%\}'
    content = re.sub(pattern, r'\1', content, flags=re.DOTALL)

    # 3. Replace "Teacher Check" with "Activity Completed"
    content = content.replace('Teacher Check', 'Activity Completed')

    # 4. Make text darker and slightly bigger
    content = content.replace('text-slate-900', 'text-slate-950')
    content = content.replace('text-slate-800', 'text-slate-900')
    content = content.replace('text-base', 'text-lg')

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print(f"Updated {filepath}")
