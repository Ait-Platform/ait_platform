import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <h3 class="text-xl font-bold text-slate-900" id="modal-title">Record Manual Transaction</h3>
          <p class="mt-2 text-sm text-slate-500">Log a manual payment (credit) or refund (debit) for {{ debtor.name }}.</p>
        </div>
        <div class="p-6 space-y-4">
          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Type</label>
            <select name="kind" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
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
            <label class="block text-sm font-bold text-slate-700 mb-1">Reference</label>
            <input type="text" name="ref" value="EFT" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Description</label>
            <input type="text" name="description" value="Thank you for this payment" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
        </div>
        <div class="px-6 py-4 bg-slate-50 flex justify-end gap-3 rounded-b-xl border-t border-slate-100">
          <button type="button" onclick="document.getElementById('add-payment-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-100 transition">Cancel</button>
          <button type="submit" class="px-5 py-2.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition">Save Transaction</button>
        </div>'''

content = re.sub(
    r"<h3 class=\"text-xl font-bold text-slate-900\" id=\"modal-title\">Record Manual Payment</h3>.*?<button type=\"submit\" class=\"px-5 py-2\.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition\">Save Payment</button>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

# And also replace the button that opens the modal
content = content.replace("Record Payment", "Record Transaction")

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
