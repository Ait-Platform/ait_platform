import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''<a href="{{ url_for('mechanic_bp.job_card_detail', id=job.id) }}" class="text-indigo-600 hover:text-indigo-900 font-semibold bg-indigo-50 px-3 py-1 rounded-md transition hover:bg-indigo-100 border border-indigo-100">
                      Open Hub &rarr;
                    </a>
                    {% if job.status == 'Quote' %}
                    <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job.id) }}" class="inline m-0 ml-2">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                      <button type="submit" class="text-green-700 hover:text-green-900 font-semibold bg-green-50 px-3 py-1 rounded-md transition hover:bg-green-200 border border-green-200 shadow-sm" title="Mark as Accepted">
                        <i class="fas fa-check mr-1"></i> Accept
                      </button>
                    </form>
                    <form method="POST" action="{{ url_for('mechanic_bp.reject_quote', id=job.id) }}" class="inline m-0 ml-1">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                      <input type="hidden" name="reason" value="Rejected from Dashboard">
                      <button type="submit" class="text-slate-700 hover:text-slate-900 font-semibold bg-white px-3 py-1 rounded-md transition hover:bg-slate-50 border border-slate-300 shadow-sm" title="Mark as Rejected">
                        <i class="fas fa-times mr-1"></i> Reject
                      </button>
                    </form>
                    {% endif %}'''

content = content.replace('''<a href="{{ url_for('mechanic_bp.job_card_detail', id=job.id) }}" class="text-indigo-600 hover:text-indigo-900 font-semibold bg-indigo-50 px-3 py-1 rounded-md transition hover:bg-indigo-100 border border-indigo-100">
                      Open Hub &rarr;
                    </a>''', replacement)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)

