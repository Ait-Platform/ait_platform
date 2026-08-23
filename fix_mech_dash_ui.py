import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add Bank Details textarea
old_tc = '''<!-- ALWAYS VISIBLE: Terms & Conditions -->'''
new_bank_and_tc = '''<!-- Banking Details -->
            <div class="border-t border-slate-200 pt-6 mb-6">
              <label class="block text-sm font-bold text-slate-700 mb-1">Banking Details (For Payments)</label>
              <p class="text-xs text-slate-500 mb-2">These details will appear on your Invoices and Statements.</p>
              <textarea name="bank_details" rows="3" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition" placeholder="Bank Name: ...&#10;Account Name: ...&#10;Account No: ...&#10;Branch Code: ...">{{ active_shop.bank_details if active_shop else '' }}</textarea>
            </div>
            
            <!-- ALWAYS VISIBLE: Terms & Conditions -->'''
            
content = content.replace(old_tc, new_bank_and_tc)

# Remove the "Go to Debtors Setup" warning
old_warn = '''<!-- Missing Bank Account Warning -->
          {% if not has_bank %}
          <div class="mb-6 bg-amber-50 border-l-4 border-amber-500 p-4 rounded-r shadow-sm">
            <div class="flex">
              <div class="flex-shrink-0">
                <i class="fas fa-exclamation-triangle text-amber-500 mt-0.5"></i>
              </div>
              <div class="ml-3">
                <h3 class="text-sm font-bold text-amber-800">Important Next Step</h3>
                <div class="mt-1 text-sm text-amber-700">
                  <p>Your shop profile is active, but you haven't linked a Bank Account in the Debtors module yet. You need this to receive payments on your Statements of Account (SOA).</p>
                  <a href="{{ url_for('debtors_bp.profile') }}" class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 transition shadow-sm inline-block font-bold mt-2">Go to Debtors Setup</a>
                </div>
              </div>
            </div>
          </div>
          {% endif %}'''
          
content = content.replace(old_warn, "")

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
