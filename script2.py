import os
import re

directory = r'D:\Users\yeshk\Documents\ait_platform\templates\subject_home'

for i in range(1, 11):
    filepath = os.path.join(directory, f'chapter{i}_practical.html')
    if not os.path.exists(filepath):
        continue
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # The block starts with "<!-- Final Competency Check -->" and ends before "<!-- Submit -->"
    # We want to wrap the entire thing in {% if is_teacher_scoring %}
    
    # First, let's remove the {% if is_teacher_scoring %} logic we put inside it earlier
    # because now we are wrapping the whole block.
    # Actually, we can just replace the whole block from "<!-- Final Competency Check -->" to "<!-- Submit -->"
    
    pattern = r'(<!-- Final Competency Check -->[\s\S]*?)(<!-- Submit -->)'
    
    def replacer(match):
        block = match.group(1)
        # remove the inner {% if is_teacher_scoring %} and {% else %} parts we injected earlier
        block = re.sub(r'\{%\s*if is_teacher_scoring\s*%\}', '', block)
        block = re.sub(r'\{%\s*else\s*%\}[\s\S]*?\{%\s*endif\s*%\}', '', block)
        # wrap the whole block
        return '{% if is_teacher_scoring %}\n' + block + '{% endif %}\n' + match.group(2)
        
    content = re.sub(pattern, replacer, content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
