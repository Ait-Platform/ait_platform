import os
import re

directory = 'templates/program_cptd/modules'

# Regular expressions
regex_title = re.compile(r'<h1[^>]*>(.*?)</h1>', re.DOTALL)
regex_content = re.compile(r'<div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-8 mb-6">(.*?)</div>\s*<div class="bg-indigo-50', re.DOTALL)
regex_checkpoint = re.compile(r'<div class="bg-indigo-50 border border-indigo-200 rounded-2xl p-8">(.*?)</div>\s*</div>\s*</div>\s*{% endblock %}', re.DOTALL)

for filename in os.listdir(directory):
    if filename.startswith('reading_') and filename.endswith('.html'):
        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        title_match = regex_title.search(content)
        content_match = regex_content.search(content)
        checkpoint_match = regex_checkpoint.search(content)
        
        if title_match and content_match and checkpoint_match:
            title = title_match.group(1).strip()
            body_content = content_match.group(1).strip()
            checkpoint = checkpoint_match.group(1).strip()
            
            # Reconstruct the file
            new_content = f'''{{% extends "layout.html" %}}
{{% block title %}}Reading Module - {title}{{% endblock %}}

{{% block flashes %}}{{% endblock %}}

{{% block content %}}
<div class="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-100 mb-12">
    <div class="h-2 w-full bg-blue-600"></div>
    
    <div class="p-6 sm:p-8">
        {{% include "partials/flash_messages.html" %}}
        
        <div class="flex justify-between items-center mb-6">
            <h1 class="text-2xl font-bold text-gray-900">{title}</h1>
            <a href="{{{{ url_for('cptd_bp.reading_timetable') }}}}" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200">
                Back to Map
            </a>
        </div>

        <div class="mb-8">
            {body_content}
        </div>

        <div class="bg-blue-50 border border-blue-200 rounded-2xl p-8">
            {checkpoint}
        </div>
    </div>
</div>
{{% endblock %}}'''
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Updated {filename}")
        else:
            print(f"Could not parse {filename}")
