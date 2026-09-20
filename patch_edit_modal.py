with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

def replace_header(text, old_color, new_color_class):
    old_str = f"""<div class="flex justify-between items-center mb-2">
                        <div class="text-[10px] font-bold text-{old_color}-400 uppercase tracking-widest">{{{{ seat.qualifier }}}}</div>"""
    
    new_str = f"""<div class="flex justify-between items-center mb-2">
                        <div class="flex items-center space-x-2">
                            <div class="text-[10px] font-bold text-{old_color}-400 uppercase tracking-widest">{{{{ seat.qualifier }}}}</div>
                            <button onclick="openEditSeatModal({{{{ seat.id }}}}, '{{{{ seat.title|replace('\'', '\\\'') }}}}', '{{{{ seat.group_level }}}}', '{{{{ seat.qualifier }}}}', '{{{{ seat.duty|default('committee_member') }}}}')" class="text-{old_color}-300 hover:text-{old_color}-600 transition" title="Edit Seat"><i class="fas fa-pencil-alt text-xs"></i></button>
                        </div>"""
    return text.replace(old_str, new_str)

text = replace_header(text, "indigo", "indigo")
text = replace_header(text, "emerald", "emerald")
text = replace_header(text, "amber", "amber")

# Add the editSeatModal
modal_html = """<!-- Edit Seat Modal -->
<div id="editSeatModal" class="hidden fixed inset-0 bg-slate-900/50 z-50 flex items-center justify-center backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-xl max-w-md w-full overflow-hidden">
        <div class="p-6 border-b border-slate-100 flex justify-between items-center">
            <h3 class="font-bold text-lg text-slate-800">Edit Blueprint Seat</h3>
            <button type="button" onclick="document.getElementById('editSeatModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
        </div>
        <form method="POST" action="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="p-6 space-y-4">
            <input type="hidden" name="action" value="edit_seat">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <input type="hidden" name="seat_id" id="edit_seat_id">
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Platform Permission (Duty)</label>
                <select name="duty" id="edit_duty" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 mb-4">
                    <option value="manager">Manager (Full Access)</option>
                    <option value="committee_member">Committee Member (Standard)</option>
                    <option value="receptionist">Receptionist (Intake Desk)</option>
                    <option value="owner">Ratepayer (Base Access)</option>
                </select>
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Group Level</label>
                <select name="group_level" id="edit_group_level" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="CORE_EXCO">Core Executive Committee</option>
                    <option value="SECOND_GROUP">Sub-Committees & General</option>
                    <option value="OPERATIONS">Community & Operations (Managers, Staff, Providers)</option>
                </select>
            </div>

            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Seat Title</label>
                <input type="text" name="title" id="edit_title" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Qualifier (Optional Badge)</label>
                <input type="text" name="qualifier" id="edit_qualifier" class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
            </div>
            
            <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                <button type="button" onclick="document.getElementById('editSeatModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                <button type="submit" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Save Changes</button>
            </div>
        </form>
    </div>
</div>

<script>
function openEditSeatModal(id, title, groupLevel, qualifier, duty) {
    document.getElementById('edit_seat_id').value = id;
    document.getElementById('edit_title').value = title;
    document.getElementById('edit_group_level').value = groupLevel;
    document.getElementById('edit_qualifier').value = qualifier && qualifier !== 'None' ? qualifier : '';
    document.getElementById('edit_duty').value = duty;
    document.getElementById('editSeatModal').classList.remove('hidden');
}
</script>
"""

# Append modal_html just before closing body or end of block content
end_block = text.rfind('{% endblock %}')
if end_block != -1:
    text = text[:end_block] + modal_html + text[end_block:]

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated organogram with Edit Seat functionality")
