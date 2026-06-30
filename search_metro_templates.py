import os
search_dir = r'D:\Users\yeshk\Documents\ait_platform\templates'
for root, dirs, files in os.walk(search_dir):
    for file in files:
        if file.endswith('.html'):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if 'metro' in content.lower() or 'billing' in content.lower():
                        print(f'Found in: {filepath}')
            except Exception:
                pass
