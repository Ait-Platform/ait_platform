import os

target_dir = 'templates'
to_replace = 'yoco_bp.yoco_start'
replacement = 'paddle_bp.paddle_start'

for root, _, files in os.walk(target_dir):
    for file in files:
        if file.endswith('.html'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            if to_replace in content:
                content = content.replace(to_replace, replacement)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Updated {filepath}")
