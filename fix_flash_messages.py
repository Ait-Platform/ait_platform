import os
import glob

templates = ['soa_dashboard.html', 'soa_map.html', 'soa_tenants.html', 'soa_generate.html']

for t in templates:
    path = f'templates/program_billing/{t}'
    if not os.path.exists(path):
        continue
        
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
        
    if '{% include "partials/flash_messages.html" %}' in text:
        # Remove it from outside
        text = text.replace('    {% include "partials/flash_messages.html" %}\n\n    <div class="bg-white', '    <div class="bg-white')
        text = text.replace('    {% include "partials/flash_messages.html" %}\n    <div class="bg-white', '    <div class="bg-white')
        text = text.replace('    {% include "partials/flash_messages.html" %}\n\n    <div class="bg-white', '    <div class="bg-white')
        
        # In case there are stray newlines
        while '    {% include "partials/flash_messages.html" %}\n\n\n    <div class="bg-white' in text:
            text = text.replace('    {% include "partials/flash_messages.html" %}\n\n\n    <div class="bg-white', '    <div class="bg-white')
            
        text = text.replace('    {% include "partials/flash_messages.html" %}\n', '')
            
        # Add it inside if not already there
        if '      {% include "partials/flash_messages.html" %}\n      <div class="h-2' not in text:
            text = text.replace('    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">\n      <div class="h-2', '    <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">\n      {% include "partials/flash_messages.html" %}\n      <div class="h-2')
            
        with open(path, 'w', encoding='utf-8') as f:
            f.write(text)
            
print('Updated flash messages placement')
