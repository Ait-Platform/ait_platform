import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_buttons = '''          <div class="flex items-center justify-end gap-2 flex-wrap">
            {% if job_card.status == 'Quote' %}
              <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job_card.id) }}" class="inline m-0">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Mark as Accepted</button>
              </form>
              <button onclick="document.getElementById('reject-quote-modal').classList.remove('hidden')" class="px-4 py-2 border border-slate-300 bg-white text-slate-700 font-bold rounded-lg hover:bg-slate-50 shadow-sm transition text-sm">Mark as Rejected</button>
            {% elif job_card.status == 'Awaiting Deposit' %}'''

new_buttons = '''          <div class="flex items-center justify-end gap-2 flex-wrap">
            {% if job_card.status == 'Quote' %}
              <a href="{{ url_for('mechanic_bp.edit_quote', id=job_card.id) }}" class="px-4 py-2 bg-slate-800 text-white font-bold rounded-lg hover:bg-slate-900 shadow-sm transition text-sm mr-2">Edit Quote</a>
              <form method="POST" action="{{ url_for('mechanic_bp.accept_quote', id=job_card.id) }}" class="inline m-0">
                  <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                  <button type="submit" class="px-4 py-2 bg-green-600 text-white font-bold rounded-lg hover:bg-green-700 shadow-sm transition text-sm">Mark as Accepted</button>
              </form>
              <button onclick="document.getElementById('reject-quote-modal').classList.remove('hidden')" class="px-4 py-2 border border-slate-300 bg-white text-slate-700 font-bold rounded-lg hover:bg-slate-50 shadow-sm transition text-sm">Mark as Rejected</button>
            {% elif job_card.status == 'Awaiting Deposit' %}'''

content = content.replace(old_buttons, new_buttons)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
