with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# I will replace everything between `<header class="ui-header-2row">` ... `<!-- MODALS -->` with the new table.
import re

new_content = """<header class="ui-header-2row">
    <!-- Row 1: Title & Back Button -->
    <div class="ui-header-2row-top">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight ui-header-2row-title">Organogram & Member Assignment</h1>
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center px-4 py-2 border border-slate-300 shadow-sm text-sm font-bold rounded-lg text-slate-700 bg-white hover:bg-slate-50 transition">
            <i class="fas fa-arrow-left mr-2 text-slate-400"></i> Back to Switchboard
        </a>
    </div>
    
    <!-- Row 2: Subtitle & Actions -->
    <div class="ui-header-2row-bottom">
        <div class="ui-header-subtitle">
            <p class="text-sm text-slate-500 max-w-2xl">Define organizational seats, map system permissions, and assign verified members.</p>
        </div>
        <div class="ui-header-actions">
            <button onclick="document.getElementById('addSeatModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition" style="white-space: nowrap;">
                <i class="fas fa-plus mr-2"></i> Add New Seat
            </button>
        </div>
    </div>
</header>

{% include 'partials/flash_messages.html' %}

<!-- DATA TABLE -->
<div class="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden mb-12">
    <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse">
            <thead>
                <tr class="bg-slate-50 border-b border-slate-200 text-xs font-black text-slate-500 uppercase tracking-wider">
                    <th class="p-4">Group Level</th>
                    <th class="p-4">Seat Title</th>
                    <th class="p-4">System Key</th>
                    <th class="p-4">Assigned Member</th>
                    <th class="p-4 text-right">Actions</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-100 text-sm">
                {% set all_seats = core_seats + second_seats + operations_seats %}
                {% for seat in all_seats %}
                <tr class="hover:bg-slate-50 transition group">
                    <td class="p-4 align-middle">
                        {% if seat.group_level == 'CORE_EXCO' %}
                        <span class="inline-flex items-center px-2 py-1 rounded-md text-xs font-bold bg-indigo-50 text-indigo-700 border border-indigo-100"><i class="fas fa-chess-king mr-1.5"></i> Core ExCo</span>
                        {% elif seat.group_level == 'SECOND_GROUP' %}
                        <span class="inline-flex items-center px-2 py-1 rounded-md text-xs font-bold bg-emerald-50 text-emerald-700 border border-emerald-100"><i class="fas fa-users mr-1.5"></i> Sub-Committee</span>
                        {% else %}
                        <span class="inline-flex items-center px-2 py-1 rounded-md text-xs font-bold bg-amber-50 text-amber-700 border border-amber-100"><i class="fas fa-hard-hat mr-1.5"></i> Operations</span>
                        {% endif %}
                    </td>
                    <td class="p-4 align-middle">
                        <div class="font-bold text-slate-900">{{ seat.title }}</div>
                        {% if seat.qualifier %}
                        <div class="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-0.5">{{ seat.qualifier }}</div>
                        {% endif %}
                    </td>
                    <td class="p-4 align-middle">
                        <span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-slate-800 text-white shadow-sm uppercase tracking-wider">
                            <i class="fas fa-key mr-1.5 opacity-70"></i> {{ seat.duty|default('committee_member')|replace('_', ' ')|title }}
                        </span>
                    </td>
                    <td class="p-4 align-middle">
                        {% if seat.member %}
                        <div class="flex items-center space-x-3">
                            <div class="w-8 h-8 rounded-full bg-slate-100 overflow-hidden shrink-0">
                                {% if seat.member.photo_url %}
                                <img src="{{ seat.member.photo_url }}" class="w-full h-full object-cover">
                                {% else %}
                                <i class="fas fa-user text-slate-300 w-full h-full flex items-center justify-center text-xs"></i>
                                {% endif %}
                            </div>
                            <div>
                                <div class="font-bold text-slate-900 text-sm leading-tight">{{ seat.member.name }}</div>
                                <div class="text-xs text-slate-500">{{ seat.member.email }}</div>
                            </div>
                        </div>
                        {% else %}
                        <div class="inline-flex items-center px-2 py-1 rounded bg-rose-50 text-rose-600 text-xs font-bold border border-rose-100">
                            <i class="fas fa-exclamation-circle mr-1"></i> VACANT
                        </div>
                        {% endif %}
                    </td>
                    <td class="p-4 align-middle text-right space-x-2">
                        <!-- Actions -->
                        <button onclick='openEditSeatModal({{ seat.id }}, `{{ seat.title }}`, `{{ seat.group_level }}`, `{{ seat.qualifier }}`, `{{ seat.duty|default("committee_member") }}`)' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-indigo-600 hover:border-indigo-200 hover:bg-indigo-50 shadow-sm transition" title="Edit Seat">
                            <i class="fas fa-pencil-alt text-xs"></i>
                        </button>
                        
                        <button onclick='openAssignModal(`{{ seat.title }}`)' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-emerald-600 hover:border-emerald-200 hover:bg-emerald-50 shadow-sm transition" title="Assign Member">
                            <i class="fas fa-user-plus text-xs"></i>
                        </button>

                        {% if seat.member %}
                        <button onclick='openPhotoModal({{ seat.member.id }})' class="inline-flex items-center justify-center w-8 h-8 rounded bg-white border border-slate-200 text-slate-500 hover:text-amber-600 hover:border-amber-200 hover:bg-amber-50 shadow-sm transition" title="Upload Photo">
                            <i class="fas fa-camera text-xs"></i>
                        </button>
                        {% else %}
                        <button disabled class="inline-flex items-center justify-center w-8 h-8 rounded bg-slate-50 border border-slate-100 text-slate-300 cursor-not-allowed" title="Assign a member first">
                            <i class="fas fa-camera text-xs"></i>
                        </button>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>

<!-- MODALS -->"""

pattern = r'<header class="ui-header-2row">.*?<!-- MODALS -->'
text = re.sub(pattern, new_content, text, flags=re.DOTALL)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated organogram to datatable layout")
