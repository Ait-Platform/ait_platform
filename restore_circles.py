import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Revert my big Send to Client button changes and restore the little circles properly
# First remove the big Send to Client button
content = re.sub(
    r"<a href=\"\{\{ url_for\('mechanic_bp\.email_document', id=job_card\.id\) \}\}\" class=\"px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition text-sm mr-2\"><i class=\"fas fa-paper-plane mr-2\"></i>Send to Client</a>\s*",
    "",
    content
)

# Then restore the small Email circle that I ripped out
content = content.replace(
    '''            {% else %}
              <a href="{{ url_for('mechanic_bp.download_document', id=job_card.id) }}" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition" title="Download PDF">
                <i class="fas fa-file-pdf"></i>
              </a>
              
            {% endif %}''',
    '''            {% else %}
              <a href="{{ url_for('mechanic_bp.download_document', id=job_card.id) }}" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition" title="Download PDF">
                <i class="fas fa-file-pdf"></i>
              </a>
              <a href="{{ url_for('mechanic_bp.email_document', id=job_card.id) }}" class="w-10 h-10 rounded-full bg-indigo-50 flex items-center justify-center text-indigo-600 hover:bg-indigo-100 hover:text-indigo-800 transition" title="Email {{ 'SOA' if job_card.status in ['Approved', 'Billed'] else 'Quote' }}">
                <i class="fas fa-paper-plane"></i>
              </a>
            {% endif %}'''
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
