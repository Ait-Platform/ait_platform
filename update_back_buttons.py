import os
import re

target_class = 'inline-flex items-center text-sm font-medium text-slate-700 hover:text-slate-900 transition bg-slate-100 hover:bg-slate-200 px-4 py-2 rounded-lg border border-slate-300 shadow-sm'

for root, dirs, files in os.walk('templates'):
    for f in files:
        if f.endswith('.html') and 'about' in f.lower():
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Find the back button link
            pattern = re.compile(r'<a[^>]*href=\"{{\s*url_for\(\'(?:public_bp|home_bp)\.welcome\'\)\s*}}\"[^>]*>.*?Back.*?</a>', re.DOTALL | re.IGNORECASE)
            
            if pattern.search(content):
                def replace_func(match):
                    # Check if it has absolute positioning
                    classes = target_class
                    if 'absolute' in match.group(0):
                        classes += ' absolute top-6 right-6'
                    
                    return f'<a href=\"{{{{ url_for(\'public_bp.welcome\') }}}}\" class=\"{classes}\">\n<svg class=\"w-4 h-4 mr-1.5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 19l-7-7m0 0l7-7m-7 7h18\"></path></svg>\nBack\n</a>'
                
                new_content = pattern.sub(replace_func, content)
                
                with open(path, 'w', encoding='utf-8') as file:
                    file.write(new_content)
                print(f'Updated {path}')
