import glob
import re

for file_path in glob.glob('templates/subject_home/chapter*_practical.html'):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Replace list-disc with list-decimal
    # Pattern: <ul class="list-disc[^>]*> ... </ul>
    
    def replacer(match):
        ul_start = match.group(1)
        inner = match.group(2)
        # Change ul to ol and list-disc to list-decimal
        ol_start = ul_start.replace('<ul', '<ol').replace('list-disc', 'list-decimal')
        return f'{ol_start}{inner}</ol>'
        
    pattern = r'(<ul class="list-disc[^>]*>)(.*?)</ul>'
    
    new_content = re.sub(pattern, replacer, content, flags=re.DOTALL)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
        
    print(f'Updated {file_path}')
