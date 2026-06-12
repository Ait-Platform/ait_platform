import os
d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('chapter_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
        
        modified = False
        if "'HOME™' in session.get('enrolled_programs'" in content:
            content = content.replace(
                "'HOME™' in session.get('enrolled_programs', '')",
                "'home' in session.get('enrolled_subjects', [])"
            )
            modified = True
            
        if "url_for('school_page'" in content:
            content = content.replace(
                "url_for('school_page', name='math')",
                "url_for('home_bp.subject_home')"
            )
            modified = True
            
        if modified:
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)
print("Updated all old placeholders in chapter files.")
