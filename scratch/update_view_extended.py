import re

with open('templates/uip/interactions/view.html', 'r', encoding='utf-8') as f:
    text = f.read()

new_sections = """
            <!-- Work Orders & Municipality -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <!-- Providers -->
                <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                    <div class="p-4 border-b border-slate-200 bg-slate-50 flex justify-between items-center">
                        <h3 class="font-bold text-slate-800"><i class="fas fa-truck mr-2 text-indigo-600"></i> Work Orders</h3>
                    </div>
                    <div class="divide-y divide-slate-100 p-4">
                        {% for wo in interaction.work_orders %}
                        <div class="mb-2">
                            <span class="text-xs font-bold text-slate-500">{{ wo.reference }}</span>
                            <div class="font-bold text-sm">{{ wo.provider.name }}</div>
                            <span class="bg-slate-100 text-slate-600 text-xs px-2 py-0.5 rounded">{{ wo.status }}</span>
                        </div>
                        {% else %}
                        <p class="text-xs text-slate-400 italic">No providers assigned.</p>
                        {% endfor %}
                    </div>
                </div>

                <!-- Municipality -->
                <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                    <div class="p-4 border-b border-slate-200 bg-slate-50 flex justify-between items-center">
                        <h3 class="font-bold text-slate-800"><i class="fas fa-building mr-2 text-indigo-600"></i> Municipality</h3>
                    </div>
                    <div class="divide-y divide-slate-100 p-4">
                        {% for ref in interaction.municipal_referrals %}
                        <div class="mb-2">
                            <div class="font-bold text-sm">{{ ref.department }}</div>
                            <div class="text-xs text-slate-500">Ref: {{ ref.municipality_reference }}</div>
                            <span class="bg-amber-100 text-amber-800 text-xs px-2 py-0.5 rounded">{{ ref.status }}</span>
                        </div>
                        {% else %}
                        <p class="text-xs text-slate-400 italic">Not escalated.</p>
                        {% endfor %}
                    </div>
                </div>
            </div>
"""

# Insert it before the Right Column closing div
text = text.replace('<!-- Right Column: Meta & Audit -->', new_sections + '\n        <!-- Right Column: Meta & Audit -->')

with open('templates/uip/interactions/view.html', 'w', encoding='utf-8') as f:
    f.write(text)
