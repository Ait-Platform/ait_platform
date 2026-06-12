import os
import glob

# Find all html files in templates/admin/billing
files = glob.glob('templates/admin/billing/**/*.html', recursive=True)

for f in files:
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    if 'admin_bp.billing_home' in content:
        content = content.replace('admin_bp.billing_home', 'admin_bp.readings_dashboard')
        with open(f, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"Updated {f}")

print("Done replacing.")
