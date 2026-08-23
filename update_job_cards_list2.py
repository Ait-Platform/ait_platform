import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Look for:
#                      {% endif %}
#                  </td>
regex = r'(\{\%\s*endif\s*\%\}\s*</td>)'

new_btn = '''{% endif %}
                      {% if job.status in ['Approved', 'Awaiting Deposit'] %}
                      <form method="POST" action="{{ url_for('mechanic_bp.mark_billed', id=job.id) }}" class="inline m-0 ml-1">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                        <button type="submit" class="text-slate-700 hover:text-white font-semibold bg-slate-200 px-3 py-1 rounded-md transition hover:bg-slate-800 border border-slate-300 shadow-sm" title="Mark as Completed/Billed">
                          <i class="fas fa-check-double mr-1"></i> Complete
                        </button>
                      </form>
                      {% endif %}
                  </td>'''

content = re.sub(regex, new_btn, content, count=1)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
