import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the button to say 'Verify via Mandate' and call openVerifyModal
actions_pattern = re.compile(r'onclick="openMoModal\(\'\{\{ claim\.id \}\}\', \'\{\{ claim\.user_name \}\}\'\)"[^>]*>\s*<i class="fas fa-file-signature mr-1"></i> Record Mandate')
actions_replacement = """onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')">
                                    <i class="fas fa-link mr-1"></i> Verify via Mandate"""
content = actions_pattern.sub(actions_replacement, content)

# 2. Replace the modal html and scripts
modal_pattern = re.compile(r'<!-- MO Mandate Modal -->.*?</script>', re.DOTALL)

new_modal = """<!-- Verify via Mandate Modal -->
<div id="verifyModal" class="fixed inset-0 z-[100] hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
  <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
    <div class="fixed inset-0 bg-slate-900 bg-opacity-75 transition-opacity" aria-hidden="true" onclick="closeVerifyModal()"></div>
    <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
    <div class="inline-block align-bottom bg-white rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
      <div>
        <div class="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-emerald-100">
          <i class="fas fa-link text-emerald-600 text-xl"></i>
        </div>
        <div class="mt-3 text-center sm:mt-5">
          <h3 class="text-lg leading-6 font-bold text-slate-900" id="modal-title">Verify via Existing Mandate</h3>
          <div class="mt-2 text-sm text-slate-500 text-left">
            <p>You are linking <strong id="verifyModalName" class="text-slate-800"></strong> to a foundational legal record.</p>
            <p class="mt-2">Select the pre-existing Adopted Mandate that grants this user authority.</p>
          </div>
        </div>
      </div>
      <form id="verifyMandateForm" action="{{ url_for('uip_bp.verify_claim_via_mandate', org_slug=org.slug) }}" method="POST" class="mt-5">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        <input type="hidden" name="claim_id" id="verifyModalClaimId" value=""/>
        
        <div class="mb-5">
            <label class="block text-sm font-bold text-slate-700 mb-2">Select Mandate</label>
            <select name="mandate_id" required class="block w-full border border-slate-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm">
                <option value="" disabled selected>-- Select an Adopted Mandate --</option>
                {% for mandate in adopted_resolutions %}
                <option value="{{ mandate.id }}">{{ mandate.title }} ({{ mandate.decision_date }})</option>
                {% endfor %}
            </select>
        </div>
        
        <div class="mt-5 sm:mt-6 sm:flex sm:flex-row-reverse">
          <button type="submit" name="action" value="record" class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-emerald-600 text-base font-bold text-white hover:bg-emerald-700 focus:outline-none sm:ml-3 sm:w-auto sm:text-sm">
            Admit User
          </button>
          <button type="button" onclick="closeVerifyModal()" class="mt-3 w-full inline-flex justify-center rounded-md border border-slate-300 shadow-sm px-4 py-2 bg-white text-base font-bold text-slate-700 hover:bg-slate-50 focus:outline-none sm:mt-0 sm:w-auto sm:text-sm mr-auto">
            Cancel
          </button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
function openVerifyModal(claimId, name) {
    document.getElementById('verifyModalClaimId').value = claimId;
    document.getElementById('verifyModalName').textContent = name;
    document.getElementById('verifyModal').classList.remove('hidden');
}
function closeVerifyModal() {
    document.getElementById('verifyModal').classList.add('hidden');
}
</script>"""
content = modal_pattern.sub(new_modal, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("secretary_intake updated")
