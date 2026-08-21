import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Insert letterhead at top of content block
header_find = '''    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>'''
header_replace = '''    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>

    <!-- Shop Letterhead -->
    {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
    <div class="px-6 pb-4 mb-4 text-center border-b border-slate-100 print:border-none print:pb-0 print:mb-2">
      <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url) }}" alt="Shop Letterhead" class="max-h-32 mx-auto rounded-lg shadow-sm print:shadow-none">
    </div>
    {% endif %}'''
    
if "<!-- Shop Letterhead -->" not in content:
    content = content.replace(header_find, header_replace)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done")
