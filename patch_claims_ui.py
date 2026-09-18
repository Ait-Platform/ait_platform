import re
with open("templates/program_uip/dashboards/process_claims.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace "Member Assignments" section with just a summary of claims, no portfolio inputs
old_member_assignments = """    <!-- 2. Member Details -->
    <div class="ui-form-section mb-8">
        <h2 class="text-lg font-bold text-slate-900 mb-4"><i class="fas fa-users-cog text-emerald-500 mr-2"></i> Member Assignments</h2>
        
        <div class="space-y-6">
            {% for claim in claims %}
            <input type="hidden" name="claim_ids[]" value="{{ claim.id }}"/>
            <div class="p-5 border border-slate-200 bg-slate-50 rounded-xl">
                <div class="flex justify-between items-center mb-4 pb-4 border-b border-slate-200">
                    <div>
                        <div class="font-bold text-slate-900">{{ claim.creator.name or "User" }}</div>
                        <div class="text-sm text-slate-500">{{ claim.creator.email }}</div>
                        <div class="mt-2 text-sm text-indigo-700 font-semibold">{{ claim.title }}</div>
                        <div class="text-xs text-slate-600">{{ claim.description }}</div>
                    </div>
                    <div>
                        <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-indigo-100 text-indigo-800 uppercase tracking-wider">
                            {{ claim.interaction_type.replace('_claim', '') }}
                        </span>
                    </div>
                </div>
                
                {% if claim.interaction_type == 'committee_claim' or claim.interaction_type == 'subcommittee_claim' %}
                <div>
                    <label class="block text-sm font-bold text-slate-700 mb-2">Committee Portfolio / Role <span class="text-red-500">*</span></label>
                    <input type="text" name="portfolio_{{ claim.id }}" required placeholder="e.g., Greening, Security, Treasurer..." class="w-full border border-slate-300 rounded-lg px-4 py-2 focus:ring-2 focus:ring-indigo-500 outline-none">
                    <p class="text-xs text-slate-500 mt-1">Specify their exact portfolio so it is recorded in the official resolution.</p>
                </div>
                {% else %}
                <p class="text-sm text-slate-500 italic">No additional portfolio details required for this role.</p>
                {% endif %}
            </div>
            {% endfor %}
        </div>
    </div>"""

new_member_assignments = """    <!-- 2. Included Claims -->
    <div class="ui-form-section mb-8">
        <h2 class="text-lg font-bold text-slate-900 mb-4"><i class="fas fa-users-cog text-emerald-500 mr-2"></i> Role Assignments Included</h2>
        <p class="text-sm text-slate-500 mb-4">The following claims will be bundled into this resolution. Members will independently configure their specific portfolios upon approval.</p>
        
        <div class="space-y-4">
            {% for claim in claims %}
            <input type="hidden" name="claim_ids[]" value="{{ claim.id }}"/>
            <div class="p-4 border border-slate-200 bg-slate-50 rounded-xl flex justify-between items-center">
                <div>
                    <div class="font-bold text-slate-900">{{ claim.creator.name or "User" }}</div>
                    <div class="text-sm text-slate-500">{{ claim.creator.email }}</div>
                </div>
                <div class="text-right">
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-bold bg-indigo-100 text-indigo-800 uppercase tracking-wider mb-1">
                        {{ claim.interaction_type.replace('_claim', '') }}
                    </span>
                    <div class="text-xs text-slate-600 font-semibold">{{ claim.title }}</div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>"""

if old_member_assignments in text:
    text = text.replace(old_member_assignments, new_member_assignments)
    print("Replaced Member Assignments section")
else:
    print("Failed to replace Member Assignments section")

with open("templates/program_uip/dashboards/process_claims.html", "w", encoding="utf-8") as f:
    f.write(text)
