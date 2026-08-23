import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Type</label>
            <select name="kind" id="transaction_kind" onchange="updateTransactionDescription()" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
              <option value="credit">Payment Received (Credit Client)</option>
              <option value="debit">Refund / Charge (Debit Client)</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Amount</label>
            <input type="number" step="0.01" min="0.01" name="amount" required class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Date</label>
            <input type="date" name="date" required value="{{ start_date if start_date else '' }}" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
          
          <div>
            <button type="button" onclick="document.getElementById('advanced_fields').classList.toggle('hidden')" class="text-sm text-indigo-600 hover:text-indigo-800 font-semibold">+ Add Custom Reference or Note</button>
          </div>
          
          <div id="advanced_fields" class="hidden space-y-4 pt-2 border-t border-slate-100 mt-2">
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Reference</label>
                <input type="text" name="ref" value="EFT" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Description</label>
                <input type="text" name="description" id="transaction_desc" value="Thank you for this payment" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
              </div>
          </div>
        </div>
        <div class="px-6 py-4 bg-slate-50 flex justify-end gap-3 rounded-b-xl border-t border-slate-100">'''

content = re.sub(
    r"<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Type</label>.*?<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Description</label>\s*<input type=\"text\" name=\"description\" id=\"transaction_desc\" value=\"Thank you for this payment\" class=\"block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2\.5\">\s*</div>\s*</div>\s*<div class=\"px-6 py-4 bg-slate-50 flex justify-end gap-3 rounded-b-xl border-t border-slate-100\">",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
