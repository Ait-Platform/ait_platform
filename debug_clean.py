with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re
# We know the first modal is inside {% block flashes %} right after {% block flashes %}\n\n\n  <!-- Manual Setup Modal -->\n
# Let's clean the entire {% block flashes %} to be empty.
flashes_block = '''{% block flashes %}


  <!-- Manual Setup Modal -->'''
new_flashes = '''{% block flashes %}
{% endblock %}'''

content = content.replace(flashes_block, new_flashes)

# Wait, we need to be careful. Let's just find the first <div id="manual-setup-modal" and delete it and its contents until   </div>\n  </div>
# Better: just rewrite the file safely.
