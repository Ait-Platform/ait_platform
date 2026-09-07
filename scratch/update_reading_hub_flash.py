import re

html_path = 'templates/program_sace/reading_hub.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# I want to insert the flashes after the Header block.
flash_block = '''
        <!-- Flash Messages INSIDE the tile -->
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                <div class="bg-white px-8 py-4 border-l border-r border-slate-200">
                {% for category, message in messages %}
                    <div class="p-4 rounded-lg font-bold border mb-2 
                        {% if category == 'error' %}bg-red-50 text-red-700 border-red-200
                        {% elif category == 'success' %}bg-emerald-50 text-emerald-700 border-emerald-200
                        {% else %}bg-indigo-50 text-indigo-700 border-indigo-200{% endif %}">
                        {{ message }}
                    </div>
                {% endfor %}
                </div>
            {% endif %}
        {% endwith %}
'''

# Find the end of the header
text = text.replace('        </div>\n\n        \n        <!-- Testing Mode Reset Modal -->', '        </div>\n' + flash_block + '\n        <!-- Testing Mode Reset Modal -->')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
