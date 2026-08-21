import re

with open('templates/layout.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add the Workflow Map button before the Back button
button_code = '''
            {% if request.endpoint and request.endpoint.startswith('mechanic_bp') %}
              <button onclick="document.getElementById('pt-help-modal').classList.remove('hidden')" class="text-sm font-bold text-indigo-600 bg-indigo-50 border border-indigo-200 px-3 py-1.5 rounded hover:bg-indigo-100 transition shadow-sm flex items-center">
                <i class="fas fa-map-signs mr-2"></i> Workflow Map
              </button>
            {% endif %}'''

# We find <div class="ml-auto">
idx = content.find('<div class="ml-auto">')
if idx != -1:
    content = content[:idx] + '<div class="ml-auto flex items-center gap-3">' + button_code + content[idx+21:]
else:
    print("Could not find ml-auto")

# 2. Add the modal include right before </body>
modal_include = '''
  {% if request.endpoint and request.endpoint.startswith('mechanic_bp') %}
    {% include "program_mechanic/help_modal.html" ignore missing %}
  {% endif %}
'''
content = content.replace('</body>', modal_include + '\n</body>')

with open('templates/layout.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated layout.html successfully")
