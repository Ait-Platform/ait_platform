import re

with open('templates/program_cptd/hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

new_section = '''
    <!-- Compliance Section -->
    <div class="mt-12 bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden hover:shadow-md transition flex flex-col sm:flex-row">
      <div class="w-full sm:w-2 bg-indigo-600"></div>
      <div class="p-8 flex-grow flex flex-col sm:flex-row justify-between items-center gap-6">
        <div>
          <h2 class="text-2xl font-bold text-slate-900 mb-2">SACE Compliance Documents</h2>
          <p class="text-slate-600">Print or download your required Annexures and evidence of platform access.</p>
        </div>
        <div>
          <a href="{{ url_for('cptd_bp.compliance_hub') }}" class="inline-flex items-center justify-center px-6 py-3 border border-transparent text-base font-bold rounded-xl text-indigo-700 bg-indigo-100 hover:bg-indigo-200 transition whitespace-nowrap shadow-sm">
            <i class="fas fa-print mr-2"></i> View Annexures
          </a>
        </div>
      </div>
    </div>
'''

# Find the end of the programme grid block
idx = content.rfind('    </div>\n  </div>')
if idx != -1:
    content = content[:idx] + '    </div>\n' + new_section + content[idx+10:]
    with open('templates/program_cptd/hub.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Replaced successfully")
else:
    print("Could not find insertion point")
