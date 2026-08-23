import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the bulky actions block with small icons
bulky_actions = '''        <!-- Row 2: Actions -->
        <div class="mb-6 text-sm text-gray-600 bg-blue-50 p-4 rounded-lg border border-blue-200">
          <p><strong>Print / Save PDF:</strong> Uses your browser to print the statement or save it as a PDF.</p>
          <p class="mt-2"><strong>Email Statement:</strong> Instantly sends this exact statement directly to the client's email inbox.</p>
        </div>
        <div class="flex flex-col sm:flex-row justify-end items-center bg-gray-50 p-4 rounded-lg border border-gray-200 gap-4">
          <button onclick="window.print()" class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 font-bold shadow-md transition flex justify-center items-center">
             <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z"></path></svg>
             Print / Save PDF
          </button>
          
          <button onclick="document.getElementById('email-soa-modal').classList.remove('hidden')" class="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-bold shadow-md transition flex justify-center items-center">
             <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>
             Email Statement
          </button>
        </div>'''

clean_icons = '''        <!-- Row 2: Actions -->
        <div class="flex justify-end items-center gap-3 mb-6">
          <button onclick="window.print()" class="w-10 h-10 rounded-full bg-slate-100 flex items-center justify-center text-slate-600 hover:bg-slate-200 hover:text-slate-800 transition" title="Print / Save PDF">
            <i class="fas fa-print"></i>
          </button>
          <button onclick="document.getElementById('email-soa-modal').classList.remove('hidden')" class="w-10 h-10 rounded-full bg-blue-50 flex items-center justify-center text-blue-600 hover:bg-blue-100 hover:text-blue-800 transition" title="Email Statement">
            <i class="fas fa-paper-plane"></i>
          </button>
        </div>'''

if bulky_actions in content:
    content = content.replace(bulky_actions, clean_icons)
else:
    # Use regex if exact match fails due to whitespace
    content = re.sub(r'<!-- Row 2: Actions -->.*?</div>\s*</div>', clean_icons, content, flags=re.DOTALL)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
