import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_block = '''<div class="bg-amber-50 border-l-4 border-amber-500 p-6 rounded-r-xl max-w-lg mx-auto">
              <p class="font-bold text-amber-900 mb-2">Important Next Step:</p>
              <p class="text-sm text-amber-800">
                Before sending invoices, make sure to add your Bank Account details in the 
                <strong>Debtors (SOA)</strong> module. Your selected default bank account will appear at the bottom of all generated PDFs.
              </p>
            </div>'''

new_block = '''<div class="bg-amber-50 border-l-4 border-amber-500 p-6 rounded-r-xl max-w-lg mx-auto">
              <p class="font-bold text-amber-900 mb-2">Important Next Step:</p>
              <p class="text-sm text-amber-800 mb-3">
                Before sending invoices, make sure to add your Bank Account details in the 
                <strong>Debtors (SOA)</strong> module. Your selected default bank account will appear at the bottom of all generated PDFs.
              </p>
              <a href="{{ url_for('debtors_bp.profile') }}" class="inline-block px-4 py-2 bg-amber-600 text-white text-sm font-bold rounded-lg shadow-sm hover:bg-amber-700 transition">Go to Debtors Setup &rarr;</a>
            </div>'''

content = content.replace(old_block, new_block)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
