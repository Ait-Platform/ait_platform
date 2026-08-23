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
            <label class="block text-sm font-bold text-slate-700 mb-1">Reference</label>
            <input type="text" name="ref" value="EFT" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
          <div>
            <label class="block text-sm font-bold text-slate-700 mb-1">Description</label>
            <input type="text" name="description" id="transaction_desc" value="Thank you for this payment" class="block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2.5">
          </div>
        </div>
        <div class="px-6 py-4 bg-slate-50 flex justify-end gap-3 rounded-b-xl border-t border-slate-100">
          <button type="button" onclick="document.getElementById('add-payment-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-100 transition">Cancel</button>
          <button type="submit" class="px-5 py-2.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition">Save Transaction</button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
function updateTransactionDescription() {
    const kind = document.getElementById('transaction_kind').value;
    const descInput = document.getElementById('transaction_desc');
    
    if (kind === 'debit') {
        if (descInput.value === 'Thank you for this payment') {
            descInput.value = 'Refund issued';
        }
    } else {
        if (descInput.value === 'Refund issued') {
            descInput.value = 'Thank you for this payment';
        }
    }
}
</script>'''

content = re.sub(
    r"<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Type</label>\s*<select name=\"kind\" class=\"block w-full rounded-lg border border-slate-300 focus:border-indigo-500 focus:ring-indigo-500 p-2\.5\">\s*<option value=\"credit\">Payment Received \(Credit Client\)</option>\s*<option value=\"debit\">Refund / Charge \(Debit Client\)</option>\s*</select>\s*</div>.*?<button type=\"submit\" class=\"px-5 py-2\.5 rounded-lg bg-green-600 text-white font-bold hover:bg-green-700 shadow-sm transition\">Save Transaction</button>\s*</div>\s*</form>\s*</div>\s*</div>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
