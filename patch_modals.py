import re

with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update the System Key (duty) badge in the table
old_duty_badge = """<span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-slate-800 text-white shadow-sm uppercase tracking-wider">
                              <i class="fas fa-key mr-1.5 opacity-70"></i> {{ seat.duty|default('committee_member')|replace('_', ' ')|title }}
                          </span>"""

new_duty_badge = """{% set d = seat.duty|default('committee_member') %}
                          {% if d == 'manager' or d == 'owner' %}
                          <span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-rose-600 text-white shadow-sm uppercase tracking-wider">
                          {% elif d == 'committee_member' %}
                          <span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-indigo-600 text-white shadow-sm uppercase tracking-wider">
                          {% elif d == 'treasurer' or d == 'auditor' %}
                          <span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-emerald-600 text-white shadow-sm uppercase tracking-wider">
                          {% else %}
                          <span class="inline-flex items-center px-2 py-1 rounded-full text-[10px] font-bold bg-slate-600 text-white shadow-sm uppercase tracking-wider">
                          {% endif %}
                              <i class="fas fa-key mr-1.5 opacity-70"></i> {{ d|replace('_', ' ')|title }}
                          </span>"""
text = text.replace(old_duty_badge, new_duty_badge)

# 2. Update Add Seat Modal Duties
old_add_duties = """<select name="duty" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 mb-4">
                      <option value="manager">Manager (Full Access)</option>
                      <option value="committee_member" selected>Committee Member (Standard)</option>
                      <option value="receptionist">Receptionist (Intake Desk)</option>
                  </select>"""
                  
expanded_duties = """<select name="duty" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 mb-4">
                      <option value="owner">Owner / Chairperson (Full Admin)</option>
                      <option value="manager">Manager / Vice-Chair (Full Access)</option>
                      <option value="treasurer">Treasurer (Finance Access)</option>
                      <option value="committee_member" selected>Committee Member (Standard)</option>
                      <option value="auditor">Auditor (Read-Only)</option>
                      <option value="receptionist">Receptionist (Intake Desk)</option>
                      <option value="operations">Operations (Staff / Provider)</option>
                      <option value="volunteer">Volunteer (Task Based)</option>
                  </select>"""
text = text.replace(old_add_duties, expanded_duties)

# 3. Completely replace editSeatModal to use datalists, selects, and expanded duties
old_edit_modal_start = text.find('<!-- Edit Seat Modal -->')
old_edit_modal_end = text.find('function openEditSeatModal')
old_edit_modal = text[old_edit_modal_start:old_edit_modal_end]

new_edit_modal = """<!-- Edit Seat Modal -->
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
                    <option value="owner">Owner / Chairperson (Full Admin)</option>
                    <option value="manager">Manager / Vice-Chair (Full Access)</option>
                    <option value="treasurer">Treasurer (Finance Access)</option>
                    <option value="committee_member">Committee Member (Standard)</option>
                    <option value="auditor">Auditor (Read-Only)</option>
                    <option value="receptionist">Receptionist (Intake Desk)</option>
                    <option value="operations">Operations (Staff / Provider)</option>
                    <option value="volunteer">Volunteer (Task Based)</option>
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
                <label class="block text-xs font-bold text-slate-700 mb-1">Seat Title (Role & Department)</label>
                <input type="text" list="edit-seat-suggestions" name="title" id="edit_title" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                <datalist id="edit-seat-suggestions">
                    <option value="Chairperson">
                    <option value="Vice-Chairperson">
                    <option value="Treasurer">
                    <option value="Secretary">
                    <option value="Security Sub-Committee Member">
                    <option value="Greening & Environment Member">
                    <option value="Infrastructure & Maintenance Member">
                    <option value="Social & Community Member">
                    <option value="Finance & Audit Member">
                    <option value="Precinct Manager">
                    <option value="Security Provider">
                </datalist>
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Qualifier (Badge)</label>
                <select name="qualifier" id="edit_qualifier" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="Voluntary">Voluntary</option>
                    <option value="Part-Time">Part-Time</option>
                    <option value="Full-Time Paid">Full-Time Paid</option>
                    <option value="Contractor">Contractor</option>
                </select>
            </div>
            
            <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                <button type="button" onclick="document.getElementById('editSeatModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                <button type="submit" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Save Changes</button>
            </div>
        </form>
    </div>
</div>\n\n<script>\n"""

text = text.replace(old_edit_modal, new_edit_modal)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated editSeatModal and System Key colors")
