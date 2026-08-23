import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

modal_html = '''
    <!-- Record Deposit Modal -->
    <div id="record-deposit-modal" class="fixed inset-0 z-50 hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
      <div class="flex items-end justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        <div class="fixed inset-0 transition-opacity bg-slate-900 bg-opacity-75 backdrop-blur-sm" aria-hidden="true" onclick="document.getElementById('record-deposit-modal').classList.add('hidden')"></div>
        <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
        <div class="inline-block overflow-hidden text-left align-bottom transition-all transform bg-white rounded-xl shadow-xl sm:my-8 sm:align-middle sm:max-w-lg sm:w-full border border-slate-200">
          <form method="POST" action="{{ url_for('mechanic_bp.record_deposit', id=job_card.id) }}">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class="px-6 pt-6 pb-4 bg-white sm:p-6 sm:pb-4 border-b border-slate-100">
              <h3 class="text-xl font-bold text-slate-900" id="modal-title">Record Deposit</h3>
              <p class="mt-2 text-sm text-slate-500">Enter the deposit amount received. This will automatically sync to the client's Debtors ledger.</p>
            </div>
            <div class="px-6 py-4 space-y-4">
              <div>
                <label for="deposit_amount" class="block text-sm font-bold text-slate-700 mb-1">Deposit Amount ({{ currency_sym }})</label>
                <input type="number" step="0.01" min="0.01" max="{{ grand_total }}" name="deposit_amount" id="deposit_amount" required class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition" placeholder="0.00">
              </div>
            </div>
            <div class="px-6 py-4 bg-slate-50 flex justify-end gap-3 rounded-b-xl border-t border-slate-100">
              <button type="button" onclick="document.getElementById('record-deposit-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-100 transition">Cancel</button>
              <button type="submit" class="px-5 py-2.5 rounded-lg bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-sm transition flex items-center">
                <i class="fas fa-check mr-2"></i> Confirm Deposit
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
'''

content = content.replace(
    '    <!-- Contact Client Modal -->',
    modal_html + '\n    <!-- Contact Client Modal -->'
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
