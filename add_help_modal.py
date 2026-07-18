with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add the Help Button to the top Header
old_header = """        <div class="flex items-center justify-between border-b border-slate-100 pb-4">
          <div>
            <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900 tracking-tight">Phase 1: Architecture Setup</h1>
            <p class="text-sm text-slate-500 mt-1">Property: <span class="font-semibold text-slate-700">{{ property.name }}</span></p>
          </div>
          <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="inline-flex items-center text-sm font-medium text-slate-500 hover:text-slate-800 transition bg-white hover:bg-slate-50 px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Back to Dashboard
          </a>
        </div>"""

new_header = """        <div class="flex items-center justify-between border-b border-slate-100 pb-4">
          <div>
            <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900 tracking-tight">Phase 1: Architecture Setup</h1>
            <p class="text-sm text-slate-500 mt-1">Property: <span class="font-semibold text-slate-700">{{ property.name }}</span></p>
          </div>
          <div class="flex space-x-3">
            <button type="button" onclick="document.getElementById('help-modal').classList.remove('hidden')" class="inline-flex items-center text-sm font-medium text-blue-600 hover:text-blue-800 transition bg-blue-50 hover:bg-blue-100 px-4 py-2 rounded-lg border border-blue-200 shadow-sm">
              <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
              View Instructions
            </button>
            <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="inline-flex items-center text-sm font-medium text-slate-500 hover:text-slate-800 transition bg-white hover:bg-slate-50 px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
              <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
              Back to Dashboard
            </a>
          </div>
        </div>"""
content = content.replace(old_header, new_header)

# 2. Add the Help Modal at the end of the content block
help_modal = """
<!-- Help Modal -->
<div id="help-modal" class="hidden fixed inset-0 bg-slate-900 bg-opacity-50 flex items-center justify-center z-50 p-4">
  <div class="bg-white rounded-2xl shadow-xl max-w-2xl w-full max-h-[90vh] flex flex-col overflow-hidden">
    <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
      <h3 class="font-bold text-slate-800 text-lg flex items-center">
        <svg class="w-5 h-5 text-blue-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
        Architecture Mapping Guide
      </h3>
      <button onclick="document.getElementById('help-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 focus:outline-none">
        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>
    </div>
    
    <div class="p-6 overflow-y-auto space-y-6 text-sm text-slate-600">
      <div>
        <h4 class="font-bold text-slate-800 text-base mb-2">1. The Bulk Account</h4>
        <p>Identify your main Bulk Account first. Open it, and in Tab 1, check the box: <strong>"Designate as Bulk Account"</strong>.</p>
        <p class="mt-1">In Tab 2, add all the physical bulk meters (e.g., your 5 master water meters) to this account. Because you checked the box, the system knows these are your master meters.</p>
      </div>

      <div class="border-t border-slate-100 pt-4">
        <h4 class="font-bold text-slate-800 text-base mb-2">2. The Sub-Accounts</h4>
        <p>For your other accounts, leave the Bulk checkbox unchecked. When you add a water meter to one of these accounts, the dropdown will automatically show you the bulk meters you defined in Step 1.</p>
        <p class="mt-1">Simply select <strong>"Linked to: #[Meter Number]"</strong>. This explicitly maps the sub-account to that master physical meter without you having to retype the meter number.</p>
      </div>

      <div class="border-t border-slate-100 pt-4">
        <h4 class="font-bold text-red-700 text-base mb-2">3. Exceptional Cases (Stolen Meters)</h4>
        <p>If you have a meter that was stolen but is still being billed by the municipality, click the red <strong>"+ Add Exceptional Case"</strong> button at the bottom of the meters tab.</p>
        <p class="mt-1">This creates a dual-block. Enter the Stolen Municipal Meter on top, and your New Physical Replacement Meter on the bottom. The system explicitly links them together so the municipal charges perfectly map to the tenant's physical consumption.</p>
      </div>
    </div>
    
    <div class="px-6 py-4 border-t border-slate-100 bg-slate-50 flex justify-end">
      <button onclick="document.getElementById('help-modal').classList.add('hidden')" class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-bold rounded-lg shadow-sm transition">Got it!</button>
    </div>
  </div>
</div>
"""

content = content.replace("{% endblock %}", help_modal + "\n{% endblock %}")

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Help modal added to manual_capture.html")
