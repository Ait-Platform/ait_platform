import os
import re

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('test_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
            
        modified = False
        
        # Extract test number from filename (e.g. test_1.html -> 1)
        test_num_match = re.search(r'test_(\d+)\.html', f)
        if test_num_match:
            test_num = test_num_match.group(1)
            
            # Replace old url_for pattern
            old_pattern = f"url_for('submit_test_{test_num}')"
            new_pattern = f"url_for('home_bp.submit_test', test_number={test_num})"
            
            if old_pattern in content:
                content = content.replace(old_pattern, new_pattern)
                modified = True
                
        # Check if form exists but lacks CSRF
        if 'method="POST"' in content and 'csrf_token' not in content:
            content = re.sub(
                r'(<form[^>]*method="POST"[^>]*>)',
                r'\1\n      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>',
                content
            )
            modified = True
            
        if modified:
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)

print("Updated submit_test URLs and CSRF tokens in all test HTML files.")
