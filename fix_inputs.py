import os, glob, re

template_dir = r"D:\Users\yeshk\Documents\ait_platform\templates\program_budget"
old_class = 'class="w-full rounded border border-slate-300 px-3 py-2 text-sm"'
new_class = 'class="w-full rounded border-2 border-slate-400 px-3 py-2 text-sm focus:border-sky-500 focus:ring-1 focus:ring-sky-500"'

for file_path in glob.glob(os.path.join(template_dir, "*.html")):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    modified = False
    
    if old_class in content:
        content = content.replace(old_class, new_class)
        modified = True
        
    # Find the first occurrence of <input ... new_class ...> or <select ... new_class ...>
    # and add autofocus if not already there.
    if modified:
        # We need to insert autofocus before the closing > of the first such tag
        match = re.search(r'<(input|select)[^>]*?class="w-full rounded border-2 border-slate-400[^>]*?>', content)
        if match and 'autofocus' not in match.group(0):
            tag_content = match.group(0)
            new_tag_content = tag_content[:-1] + " autofocus>"
            content = content[:match.start()] + new_tag_content + content[match.end():]
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Updated {file_path}")

print("Done fixing inputs")
