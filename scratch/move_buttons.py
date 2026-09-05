import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# First, capture the current buttons section
btn_pattern = r'<div class="flex flex-wrap justify-end gap-3 mb-8 px-8 border-b border-slate-100 pb-8">(.*?)</div>\s*<div class="px-8 py-6'
btn_match = re.search(r'<div class="flex flex-wrap justify-end gap-3 mb-8 px-8 border-b border-slate-100 pb-8">(.*?)</div>\s*{% if not has_pledged %}', text, flags=re.DOTALL)
if btn_match:
    buttons_inner = btn_match.group(1).strip()
    
    # Remove old buttons section
    text = text.replace(btn_match.group(0), '{% if not has_pledged %}')
    
    # Inject it into header
    header_end = r'<!-- About the AIT Submission -->'
    
    new_row = f'''
        <div class="px-8 pb-6 flex flex-wrap justify-end gap-3 bg-white border-b border-slate-100">
            {buttons_inner}
        </div>
        <!-- About the AIT Submission -->'''
        
    text = text.replace('<!-- About the AIT Submission -->', new_row)
    
with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
