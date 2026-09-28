import re

filepath = 'templates/program_uip/dashboards/secretary_intake.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Define the new content replacing the old Pending Access Claims block
new_block = '''<h2 class="text-2xl font-bold mt-8 mb-4">Pending Access Claims</h2>

{% if mo_conflict %}
<div class="bg-red-50 border-l-4 border-red-500 p-4 mb-6 ui-card">
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

<!-- BLOCK 1: By Mandate -->
<div class="ui-card mb-8">
    <header class="ui-card-head">
        <h2>By Mandate</h2>
        <p class="text-sm text-slate-500 font-normal mt-1">If a legal mandate already exists for the applicant, verify them instantly.</p>
    </header>
    {% if open_claims %}
    <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse">
            <thead>
                <tr class="bg-slate-50 border-b border-slate-200 text-sm text-slate-600">
                    <th class="p-4 font-semibold">Applicant Name</th>
                    <th class="p-4 font-semibold">Email Address</th>
                    <th class="p-4 font-semibold">Role Requested</th>
                    <th class="p-4 font-semibold">Date Submitted</th>
                    <th class="p-4 font-semibold">Actions</th>
                </tr>
            </thead>
            <tbody>
                {% for claim in open_claims %}
                <tr class="border-b border-slate-100 hover:bg-slate-50 transition">
                    <td class="p-4 font-medium text-slate-900">{{ claim.user_name }}</td>
                    <td class="p-4 text-slate-600">{{ claim.user_email }}</td>
                    <td class="p-4">
                        <div class="text-sm font-bold text-indigo-700">{{ claim.title }}</div>
                        {% if claim.description %}
                        <div class="text-xs text-slate-500 mt-1 max-w-xs truncate" title="{{ claim.description }}">{{ claim.description }}</div>
                        {% endif %}
                    </td>
                    <td class="p-4 text-sm text-slate-500">{{ claim.created_at.strftime('%Y-%m-%d %H:%M') }}</td>
                    <td class="p-4">
                        <form method="POST" class="inline">
                            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                            <button type="button" onclick="openVerifyModal('{{ claim.id }}', '{{ claim.user_name }}')" class="inline-flex items-center px-3 py-1.5 border border-emerald-500 text-emerald-700 bg-emerald-50 hover:bg-emerald-100 font-bold text-xs rounded transition mr-2">
                                <i class="fas fa-link mr-1.5"></i> Verify via Mandate
                            </button>
                            <button type="submit" formaction="{{ url_for('uip_bp.decline_claim', org_slug=org.slug, claim_id=claim.id) }}" class="inline-flex items-center px-3 py-1.5 text-red-500 hover:text-red-700 hover:bg-red-50 font-medium text-xs rounded transition" title="Reject Request" onclick="return confirm('Are you sure you want to permanently decline this access request?');">
                                <i class="fas fa-times mr-1.5"></i> Reject
                            </button>
                        </form>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% else %}
    <div class="ui-card-body text-center py-12">
        <div class="text-slate-300 mb-4"><i class="fas fa-inbox fa-3x"></i></div>
        <h3 class="text-lg font-medium text-slate-900">No Pending Claims</h3>
        <p class="text-slate-500 mt-1">The intake desk is clear.</p>
    </div>
    {% endif %}
</div>

<!-- BLOCK 2: By Draft Resolution -->
<div class="ui-card mb-8">
    <header class="ui-card-head">
        <h2>By Draft Resolution</h2>
        <p class="text-sm text-slate-500 font-normal mt-1">If no mandate exists, select applicants and draft a resolution for the Committee to vote on.</p>
    </header>
    {% if open_claims %}
    <form id="draftResForm" method="POST" action="{{ url_for('uip_bp.draft_access_resolution', org_slug=org.slug) }}">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        <div class="overflow-x-auto">
            <table class="w-full text-left border-collapse">
                <thead>
                    <tr class="bg-slate-50 border-b border-slate-200 text-sm text-slate-600">
                        <th class="p-4 w-12"><input type="checkbox" id="selectAll" class="rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"></th>
                        <th class="p-4 font-semibold">Applicant Name</th>
                        <th class="p-4 font-semibold">Email Address</th>
                        <th class="p-4 font-semibold">Role Requested</th>
                        <th class="p-4 font-semibold">Date Submitted</th>
                    </tr>
                </thead>
                <tbody>
                    {% for claim in open_claims %}
                    <tr class="border-b border-slate-100 hover:bg-slate-50 transition">
                        <td class="p-4">
                            <input type="checkbox" name="claim_ids[]" value="{{ claim.id }}" class="claim-checkbox rounded border-slate-400 text-indigo-600 focus:ring-indigo-500 cursor-pointer w-5 h-5">
                        </td>
                        <td class="p-4 font-medium text-slate-900">{{ claim.user_name }}</td>
                        <td class="p-4 text-slate-600">{{ claim.user_email }}</td>
                        <td class="p-4">
                            <div class="text-sm font-bold text-indigo-700">{{ claim.title }}</div>
                        </td>
                        <td class="p-4 text-sm text-slate-500">{{ claim.created_at.strftime('%Y-%m-%d %H:%M') }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        <div class="ui-card-body bg-slate-50 border-t border-slate-200 flex justify-end">
            <div class="flex items-center text-slate-500 text-sm mr-4">
                <i class="fas fa-level-up-alt fa-rotate-90 mr-2 text-slate-400"></i> Select applicants to send to the voting room.
            </div>
            <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2.5 px-6 rounded-lg shadow-sm transition inline-flex items-center">
                <i class="fas fa-balance-scale mr-2"></i> Draft Resolution for Vote
            </button>
        </div>
    </form>
    {% else %}
    <div class="ui-card-body text-center py-12">
        <p class="text-slate-500 mt-1">The intake desk is clear.</p>
    </div>
    {% endif %}
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {
    const selectAll = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.claim-checkbox');
    
    if (selectAll) {
        selectAll.addEventListener('change', function() {
            checkboxes.forEach(cb => {
                cb.checked = selectAll.checked;
            });
        });
    }
});
</script>'''

# Need to replace everything from <header class="ui-card-head"> to just before <!-- Verify via Mandate Modal -->
start_str = '<header class="ui-card-head">'
end_str = '<!-- Verify via Mandate Modal -->'

start_idx = content.find(start_str)
end_idx = content.find(end_str)

if start_idx != -1 and end_idx != -1:
    # First, let's make sure we include the trailing </script> from the old block which ends before <!-- Verify via Mandate Modal -->
    content = content[:start_idx] + new_block + '\n\n' + content[end_idx:]
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: 2-block layout injected.")
else:
    print("FAILED to find indices.")
