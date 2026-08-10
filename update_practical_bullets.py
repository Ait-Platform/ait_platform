import glob
import re

for file_path in glob.glob('templates/subject_home/chapter*_practical.html'):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Replace list-none with list-decimal inside these specific uls
    # We will look for <ul class="list-none[^>]*> and replace with <ol class="list-decimal...
    def ul_replacer(match):
        ul_start = match.group(1)
        inner = match.group(2)
        ol_start = ul_start.replace('<ul', '<ol').replace('list-none', 'list-decimal list-inside')
        
        # Remove manual bullets inside inner
        # Replace <li>• (or <li>&bull; or <li>&#8226; or whatever)
        inner = re.sub(r'<li>\s*•\s*', '<li>', inner)
        
        return f'{ol_start}{inner}</ol>'
        
    pattern = r'(<ul class="list-none[^>]*>)(.*?)</ul>'
    
    new_content = re.sub(pattern, ul_replacer, content, flags=re.DOTALL)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
        
    print(f'Updated {file_path}')
