import re

with open('templates/admin/programs/fallback_dashboard.html', 'r') as f:
    content = f.read()

replacement = '''
    <div class="flex justify-center space-x-4">
        <a href="{{ url_for('admin_bp.programs_index') }}" class="inline-flex items-center rounded-lg px-6 py-2.5 text-sm font-semibold border border-transparent bg-indigo-600 text-white hover:bg-indigo-700 shadow-sm transition-colors">
            Return to Programs
        </a>
        
        {% if subj_obj and subj_obj.start_endpoint %}
        <a href="{{ url_for(subj_obj.start_endpoint) }}" class="inline-flex items-center rounded-lg px-6 py-2.5 text-sm font-semibold border border-indigo-600 bg-white text-indigo-600 hover:bg-indigo-50 shadow-sm transition-colors">
            Access Application
        </a>
        {% endif %}
    </div>
'''

content = content.replace('<a href="{{ url_for(\'admin_bp.programs_index\') }}" class="inline-flex items-center rounded-lg px-6 py-2.5 text-sm font-semibold border border-transparent bg-indigo-600 text-white hover:bg-indigo-700 shadow-sm transition-colors">\n        Return to Programs\n    </a>', replacement.strip())

with open('templates/admin/programs/fallback_dashboard.html', 'w') as f:
    f.write(content)
