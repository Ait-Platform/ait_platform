import re

filepath = 'templates/program_uip/dashboards/committee.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Add the Record Foundational Mandate button next to Create Resolution
header_pattern = re.compile(r'<a href="\{\{ url_for\(\'uip_bp\.draft_resolution\', org_slug=org\.slug\) \}\}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center" style="white-space: nowrap;">\s*<i class="fas fa-plus mr-2"></i> Create Resolution\s*</a>')
header_replacement = """<button type="button" onclick="openMandateModal()" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center mr-2" style="white-space: nowrap;">
                <i class="fas fa-file-signature mr-2"></i> Record Foundational Mandate
            </button>
            <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center" style="white-space: nowrap;">
                <i class="fas fa-plus mr-2"></i> Create Resolution
            </a>"""
content = header_pattern.sub(header_replacement, content)

# Add the modal to the bottom of the content block
modal_code = """
<!-- Record Foundational Mandate Modal -->
<div id="foundationalMandateModal" class="fixed inset-0 z-[100] hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
  <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
    <div class="fixed inset-0 bg-slate-900 bg-opacity-75 transition-opacity" aria-hidden="true" onclick="closeMandateModal()"></div>
    <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
    <div class="inline-block align-bottom bg-white rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full sm:p-6">
      <div>
        <div class="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-emerald-100 mb-4">
          <i class="fas fa-landmark text-emerald-600 text-xl"></i>
        </div>
        <div class="text-center sm:mt-2 mb-6">
          <h3 class="text-xl leading-6 font-bold text-slate-900" id="modal-title">Record Foundational Mandate</h3>
          <p class="mt-2 text-sm text-slate-500">Create an instantly ADOPTED legal record without ExCo voting.</p>
        </div>
      </div>
      <form action="{{ url_for('uip_bp.record_foundational_mandate', org_slug=org.slug) }}" method="POST" enctype="multipart/form-data">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        
        <div class="space-y-4">
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Mandate Title</label>
                <input type="text" name="title" required placeholder="e.g. Municipal Officer Appointment" class="w-full p-2 border border-slate-300 rounded-md focus:ring-emerald-500 focus:border-emerald-500">
            </div>
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Description / Details</label>
                <textarea name="description" required rows="3" class="w-full p-2 border border-slate-300 rounded-md focus:ring-emerald-500 focus:border-emerald-500" placeholder="Details of the mandate..."></textarea>
            </div>
            
            <div class="grid grid-cols-2 gap-4">
                <div>
                    <label class="block text-sm font-bold text-slate-700 mb-1">Mandate Date</label>
                    <input type="date" name="meeting_date" required class="w-full p-2 border border-slate-300 rounded-md focus:ring-emerald-500 focus:border-emerald-500">
                </div>
                <div>
                    <label class="block text-sm font-bold text-slate-700 mb-1">Location / Authority</label>
                    <input type="text" name="meeting_location" required placeholder="e.g. eThekwini Municipality" class="w-full p-2 border border-slate-300 rounded-md focus:ring-emerald-500 focus:border-emerald-500">
                </div>
            </div>
            
            <div class="pt-2 border-t border-slate-200">
                <label class="block text-sm font-bold text-slate-700 mb-1">Upload Proof (PDF)</label>
                <input type="file" name="mandate_file" accept=".pdf,.png,.jpg,.jpeg" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-emerald-50 file:text-emerald-700 hover:file:bg-emerald-100" />
                <p class="text-xs text-slate-500 mt-1">Upload the official letter, AGM minutes, or proof of mandate.</p>
            </div>
        </div>
        
        <div class="mt-6 sm:flex sm:flex-row-reverse">
          <button type="submit" class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-emerald-600 text-base font-bold text-white hover:bg-emerald-700 focus:outline-none sm:ml-3 sm:w-auto sm:text-sm">
            Record Mandate
          </button>
          <button type="button" onclick="closeMandateModal()" class="mt-3 w-full inline-flex justify-center rounded-md border border-slate-300 shadow-sm px-4 py-2 bg-white text-base font-bold text-slate-700 hover:bg-slate-50 focus:outline-none sm:mt-0 sm:w-auto sm:text-sm">
            Cancel
          </button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
function openMandateModal() {
    document.getElementById('foundationalMandateModal').classList.remove('hidden');
}
function closeMandateModal() {
    document.getElementById('foundationalMandateModal').classList.add('hidden');
}
</script>
"""

# add it before {% endblock %}
parts = content.rpartition("{% endblock %}")
content = parts[0] + modal_code + parts[1] + parts[2]

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("committee.html updated")
