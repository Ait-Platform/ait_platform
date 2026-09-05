import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Extract header
header_pattern = r'<!-- Header Section -->.*?</div>\s*<!-- About the AIT Submission -->'
header_match = re.search(header_pattern, text, flags=re.DOTALL)
if header_match:
    original_header = header_match.group(0)

# Extract action buttons
buttons_pattern = r'<div class="flex flex-wrap justify-end gap-3 mb-8 px-8 border-b border-slate-100 pb-8">.*?</div>\s*<!-- Locked State -->'
buttons_match = re.search(buttons_pattern, text, flags=re.DOTALL)
if not buttons_match:
    # try another
    buttons_pattern = r'<div class="flex flex-wrap justify-end gap-3 mb-8 px-8 border-b border-slate-100 pb-8">.*?</div>\s*{% if not has_pledged %}'
    buttons_match = re.search(buttons_pattern, text, flags=re.DOTALL)

if buttons_match:
    buttons_html = buttons_match.group(0)
    # Remove buttons from original spot
    text = text.replace(buttons_html, '')
    
    # We want to change the buttons HTML slightly to fit right under header
    new_buttons_html = buttons_html.replace('mb-8', 'mb-0').replace('pb-8', 'pb-4').replace('border-b border-slate-100', '')
    
    # Place buttons inside the header or right below it
    new_header = original_header.replace('</div>\s*<!-- About the AIT Submission -->', '')
    new_header_combined = new_header + '\n        ' + new_buttons_html + '\n        </div>\n        <!-- About the AIT Submission -->'
    text = text.replace(original_header, new_header_combined)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
