import os

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Insert conflict banner below header
conflict_banner = """
        </header>

        {% if mo_conflict %}
        <div class="bg-red-50 border-l-4 border-red-500 p-4 mb-6">
            <div class="flex">
                <div class="flex-shrink-0">
                    <i class="fas fa-exclamation-triangle text-red-500"></i>
                </div>
                <div class="ml-3">
                    <h3 class="text-sm font-medium text-red-800">Municipal Officer Conflict Detected</h3>
                    <div class="mt-2 text-sm text-red-700">
                        <p>We already have an active Municipal Officer ({{ existing_mo.name }}), but another user is applying for the position. Since only 1 Municipal Officer is allowed, recording a new mandate will replace the existing MO.</p>
                    </div>
                </div>
            </div>
        </div>
        {% endif %}
"""
content = content.replace("        </header>", conflict_banner, 1)

# 2. Modify loop for mo_claim checkbox and actions
loop_orig = """                              <td class="p-4">
                                  <input type="checkbox" name="claim_ids[]" value="{{ claim.id }}" class="claim-checkbox rounded border-slate-300 text-indigo-600 focus:ring-indigo-500">
                              </td>"""

loop_new = """                              <td class="p-4">
                                  {% if claim.type != 'mo_claim' %}
                                  <input type="checkbox" name="claim_ids[]" value="{{ claim.id }}" class="claim-checkbox rounded border-slate-300 text-indigo-600 focus:ring-indigo-500">
                                  {% else %}
                                  <i class="fas fa-star text-amber-400" title="Mandate requires direct recording"></i>
                                  {% endif %}
                              </td>"""
content = content.replace(loop_orig, loop_new)

actions_orig = """                              <td class="p-4">
                                  <button type="submit" formmethod="POST" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="text-red-500 hover:text-red-700 font-medium" title="Remove Claim" onclick="return confirm('Are you sure you want to remove this pending verification?');">
                                      <i class="fas fa-trash-alt mr-1"></i> Remove
                                  </button>
                              </td>"""

actions_new = """                              <td class="p-4">
                                  {% if claim.type == 'mo_claim' %}
                                  <button type="button" onclick="openMoModal('{{ claim.id }}', '{{ claim.user_name }}')" class="text-indigo-600 hover:text-indigo-800 font-bold text-sm mr-3">
                                      <i class="fas fa-file-signature mr-1"></i> Record Mandate
                                  </button>
                                  {% endif %}
                                  <button type="submit" formmethod="POST" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="text-red-500 hover:text-red-700 font-medium" title="Remove Claim" onclick="return confirm('Are you sure you want to remove this pending verification?');">
                                      <i class="fas fa-trash-alt mr-1"></i> Remove
                                  </button>
                              </td>"""
content = content.replace(actions_orig, actions_new)

# 3. Add modal at the very end of the file before the final endblock
modal_code = """
<!-- MO Mandate Modal -->
<div id="moModal" class="fixed inset-0 z-[100] hidden overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
  <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
    <div class="fixed inset-0 bg-slate-900 bg-opacity-75 transition-opacity" aria-hidden="true" onclick="closeMoModal()"></div>
    <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
    <div class="inline-block align-bottom bg-white rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
      <div>
        <div class="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-indigo-100">
          <i class="fas fa-file-signature text-indigo-600 text-xl"></i>
        </div>
        <div class="mt-3 text-center sm:mt-5">
          <h3 class="text-lg leading-6 font-bold text-slate-900" id="modal-title">Record Official Mandate</h3>
          <div class="mt-2 text-sm text-slate-500 text-left">
            <p>You are officially verifying <strong id="moModalName" class="text-slate-800"></strong> as the Municipal Officer.</p>
            <p class="mt-2">This is an external mandate from the Municipality. Recording this will immediately admit the MO and generate an official record in the Mandates register.</p>
          </div>
        </div>
      </div>
      <form id="moMandateForm" action="{{ url_for('uip_bp.record_mo_mandate', org_slug=org.slug) }}" method="POST" enctype="multipart/form-data" class="mt-5">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        <input type="hidden" name="claim_id" id="moModalClaimId" value=""/>
        
        <div class="mb-4">
            <label class="block text-sm font-bold text-slate-700 mb-1">Official Letter (Optional)</label>
            <input type="file" name="mandate_file" accept=".pdf,.png,.jpg,.jpeg" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100" />
            <p class="text-xs text-slate-500 mt-1">Upload the official appointment letter from the municipality to avoid any future disputes.</p>
        </div>
        
        <div class="mt-5 sm:mt-6 sm:flex sm:flex-row-reverse">
          <button type="submit" name="action" value="record" class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-indigo-600 text-base font-bold text-white hover:bg-indigo-700 focus:outline-none sm:ml-3 sm:w-auto sm:text-sm">
            Admit MO
          </button>
          <button type="submit" name="action" value="reject" class="mt-3 w-full inline-flex justify-center rounded-md border border-slate-300 shadow-sm px-4 py-2 bg-white text-base font-bold text-red-600 hover:bg-slate-50 focus:outline-none sm:mt-0 sm:w-auto sm:text-sm">
            Reject Mandate
          </button>
          <button type="button" onclick="closeMoModal()" class="mt-3 w-full inline-flex justify-center rounded-md border border-transparent px-4 py-2 bg-transparent text-base font-bold text-slate-500 hover:text-slate-700 focus:outline-none sm:mt-0 sm:w-auto sm:text-sm mr-auto">
            Cancel
          </button>
        </div>
      </form>
    </div>
  </div>
</div>

<script>
function openMoModal(claimId, name) {
    document.getElementById('moModalClaimId').value = claimId;
    document.getElementById('moModalName').textContent = name;
    document.getElementById('moModal').classList.remove('hidden');
}
function closeMoModal() {
    document.getElementById('moModal').classList.add('hidden');
}
</script>
{% endblock %}
"""

# Replace the LAST occurrence of {% endblock %}
# We can rpartition string by {% endblock %}
parts = content.rpartition("{% endblock %}")
content = parts[0] + modal_code

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated successfully")
