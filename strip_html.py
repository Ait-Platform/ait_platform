import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Insert letterhead at top of content block
header_find = '''    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>'''
header_replace = '''    <div class="px-6 pt-6 pb-2">
      {% include "partials/flash_messages.html" %}
    </div>

    <!-- Shop Letterhead -->
    {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
    <div class="px-6 pb-4 mb-4 text-center border-b border-slate-100">
      <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url) }}" alt="Shop Letterhead" class="max-h-32 mx-auto rounded-lg shadow-sm">
    </div>
    {% endif %}'''
content = content.replace(header_find, header_replace)

# 2. Remove all action buttons except Print
actions_regex = re.compile(r'<div class="flex flex-wrap items-center gap-3 print:hidden">.*?</div>', re.DOTALL)
actions_replace = '''<div class="flex flex-wrap items-center gap-3 print:hidden">
        <button onclick="window.print()" class="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">
          <i class="fas fa-print mr-2"></i> Download / Print PDF
        </button>
      </div>'''
content = actions_regex.sub(actions_replace, content)

# 3. Remove "Edit" buttons
edit_regex1 = re.compile(r'<button type="button" onclick="document\.getElementById\(''edit-client-modal''\)\.classList\.remove\(''hidden''\)".*?</button>', re.DOTALL)
content = edit_regex1.sub('', content)

edit_regex2 = re.compile(r'<button class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">\s*<i class="fas fa-edit mr-1"></i>Edit\s*</button>', re.DOTALL)
content = edit_regex2.sub('', content)

# 4. Remove the Edit Client Modal at the bottom
modal_regex = re.compile(r'<!-- Edit Client Modal -->.*{% endblock %}', re.DOTALL)
content = modal_regex.sub('{% endblock %}', content)

# 5. Remove the clickable phone link (make it plain text)
phone_regex = re.compile(r'<a href="javascript:void\(0\)".*?title="Call Client">(.*?)</a>', re.DOTALL)
content = phone_regex.sub(r'<span class="text-slate-600">\1</span>', content)

# 6. Remove the "Back" button from the Title row
back_btn_regex = re.compile(r'<a href="{{ url_for\(''mechanic_bp\.mechanic_dashboard''\) }}".*?</a>', re.DOTALL)
content = back_btn_regex.sub('', content)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Done")
