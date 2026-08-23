import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      <div class="flex items-center gap-3">
        <button onclick="document.getElementById('scheduleModal').classList.remove('hidden')" class="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 transition shadow-sm">
          <i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule</span>
        </button>
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>
      </div>'''

content = content.replace(
    '''      <div class="flex items-center gap-3">
        <a href="{{ url_for('mechanic_bp.debtors_schedule_options') }}" class="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 transition shadow-sm">
          <i class="fas fa-file-invoice-dollar"></i><span>Debtors Schedule (10 Tokens)</span>
        </a>
        <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>
      </div>''',
    replacement
)

modal_html = '''
<!-- Generate Schedule Modal -->
<div id="scheduleModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-slate-900 bg-opacity-50">
  <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden">
    <div class="bg-indigo-600 px-6 py-4 flex justify-between items-center">
      <h3 class="text-lg font-bold text-white">Generate Debtors Schedule</h3>
      <button onclick="document.getElementById('scheduleModal').classList.add('hidden')" class="text-indigo-200 hover:text-white transition">
        <i class="fas fa-times"></i>
      </button>
    </div>
    <form action="{{ url_for('mechanic_bp.generate_debtors_schedule') }}" method="POST" class="p-6">
      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
      <p class="text-sm text-slate-600 mb-4">Generate a snapshot balance sheet of all your debtors. Generating this report costs <strong class="text-slate-900">10 tokens</strong>.</p>
      
      <div class="space-y-4">
        <div>
          <label class="block text-sm font-bold text-slate-700 mb-1">Start Date (Optional)</label>
          <input type="date" name="start_date" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-indigo-500 focus:border-indigo-500">
        </div>
        <div>
          <label class="block text-sm font-bold text-slate-700 mb-1">End Date (Optional)</label>
          <input type="date" name="end_date" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-indigo-500 focus:border-indigo-500">
        </div>
      </div>
      
      <div class="mt-6 flex justify-end gap-3">
        <button type="button" onclick="document.getElementById('scheduleModal').classList.add('hidden')" class="px-4 py-2 text-sm font-bold text-slate-600 hover:text-slate-900">Cancel</button>
        <button type="submit" class="px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition">Generate (10 Tokens)</button>
      </div>
    </form>
  </div>
</div>
{% endblock %}
'''

content = content.replace('{% endblock %}', modal_html)

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
