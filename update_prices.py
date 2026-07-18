import os
import re
import glob

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content
    
    # 1. Add "30-Day Free Trial" tag in the tiles before the icon
    # We look for: <div class="bg-white rounded-xl shadow-sm flex items-center justify-center
    # and insert the tag right before it.
    
    tag = '<span class="inline-block bg-green-100 text-green-800 text-xs px-2 py-1 rounded-full font-bold mb-3 uppercase tracking-wide">30-Day Free Trial</span>\n              '
    content = re.sub(r'(<div class="[^"]*?w-12 h-12 bg-white rounded-xl shadow-sm flex items-center justify-center[^"]*?">)', lambda m: tag + m.group(1), content)
    
    # 2. Remove "ZAR " from "ZAR 100"
    content = re.sub(r'ZAR\s*100', '100', content)
    
    # 3. Replace "once-off fee" with "once-off"
    content = content.replace('once-off fee', 'once-off')
    
    # 4. Replace "Register & Pay ZAR 100" or similar with "Register to start your free trial"
    content = re.sub(r'>\s*Register & Pay (ZAR)?\s*100\s*<', '>Register to start your free trial<', content)
    content = re.sub(r'>\s*Register & Pay\s*<', '>Register to start your free trial<', content)
    
    # 5. Small note below to say they can simply login
    # Look for: <p class="text-sm text-slate-500 mt-4">\n            Already have an account? \n            <a href="{{ url_for('auth_bp.login'
    login_str = "Already have an account?"
    new_login_str = "Once registered, simply"
    
    content = content.replace("Already have an account?", "Once registered, simply")
    content = content.replace("Log in here", "log in to your dashboard")
    
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated: {filepath}")

files = glob.glob('D:/Users/yeshk/Documents/ait_platform/templates/**/price.html', recursive=True)
for f in files:
    process_file(f)

print("Done.")
