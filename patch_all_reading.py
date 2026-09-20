import os

for root, dirs, files in os.walk('templates/admin/programs/reading'):
    for f in files:
        filepath = os.path.join(root, f)
        with open(filepath, 'r', encoding='utf-8') as file:
            text = file.read()
        
        if 'admin_bp.reading_lessons' in text:
            new_text = text.replace("url_for('admin_bp.reading_lessons')", "url_for('admin_bp.lessons', subject=subject)")
            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(new_text)
            print(f"Fixed {f}")
