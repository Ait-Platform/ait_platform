import re

with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace Add Seat Modal
old_add = """<!-- Add Seat Modal -->
<div id="addSeatModal" class="hidden fixed inset-0 bg-slate-900/50 z-50 flex items-center justify-center backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-xl max-w-md w-full overflow-hidden">
        <div class="p-6 border-b border-slate-100 flex justify-between items-center">
            <h3 class="font-bold text-lg text-slate-800">Add Blueprint Seat</h3>
            <button onclick="document.getElementById('addSeatModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
        </div>
        <form method="POST" action="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="p-6 space-y-4">
            <input type="hidden" name="action" value="add_seat">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Seat Title</label>
                <input type="text" name="title" required placeholder="e.g. Head of Security" class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Group Level</label>
                <select name="group_level" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="CORE_EXCO">Core Executive Committee</option>
                    <option value="SECOND_GROUP" selected>Sub-Committees & General</option>
                    <option value="OPERATIONS">Community & Operations (Managers, Staff, Providers)</option>
                </select>
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Qualifier</label>
                <select name="qualifier" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="Voluntary">Voluntary</option>
                    <option value="Part-Time">Part-Time</option>
                    <option value="Full-Time Paid">Full-Time Paid</option>
                    <option value="Contractor">Contractor</option>
                </select>
            </div>
            
            <div class="pt-4 border-t border-slate-100 text-right">
                <button type="submit" class="ui-btn ui-btn-primary w-full">Create Seat</button>
            </div>
        </form>
    </div>
</div>"""

new_add = """<!-- Add Seat Modal -->
<div id="addSeatModal" class="hidden fixed inset-0 bg-slate-900/50 z-50 flex items-center justify-center backdrop-blur-sm">
    <div class="bg-white rounded-2xl shadow-xl max-w-md w-full overflow-hidden">
        <div class="p-6 border-b border-slate-100 flex justify-between items-center">
            <h3 class="font-bold text-lg text-slate-800">Add Blueprint Seat</h3>
            <button type="button" onclick="document.getElementById('addSeatModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
        </div>
        <form method="POST" action="{{ url_for('uip_bp.secretary_organogram', org_slug=org.slug) }}" class="p-6 space-y-4">
            <input type="hidden" name="action" value="add_seat">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Group Level</label>
                <select name="group_level" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="CORE_EXCO">Core Executive Committee</option>
                    <option value="SECOND_GROUP" selected>Sub-Committees & General</option>
                    <option value="OPERATIONS">Community & Operations (Managers, Staff, Providers)</option>
                </select>
                <p class="text-[10px] text-slate-500 mt-1">Select where this seat sits in the organogram hierarchy.</p>
            </div>

            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Seat Title (Role & Department)</label>
                <input type="text" list="seat-suggestions" name="title" required placeholder="e.g. Security Sub-Committee Member" class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                <datalist id="seat-suggestions">
                    <option value="Security Sub-Committee Member">
                    <option value="Greening & Environment Member">
                    <option value="Infrastructure & Maintenance Member">
                    <option value="Social & Community Member">
                    <option value="Finance & Audit Member">
                    <option value="Precinct Manager">
                    <option value="Security Provider">
                </datalist>
                <p class="text-[10px] text-slate-500 mt-1">Specify both the role and the sub-committee/department so it's easily identifiable (e.g., 'Finance Sub-Committee Member').</p>
            </div>
            
            <div>
                <label class="block text-xs font-bold text-slate-700 mb-1">Qualifier</label>
                <select name="qualifier" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                    <option value="Voluntary">Voluntary</option>
                    <option value="Part-Time">Part-Time</option>
                    <option value="Full-Time Paid">Full-Time Paid</option>
                    <option value="Contractor">Contractor</option>
                </select>
            </div>
            
            <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                <button type="button" onclick="document.getElementById('addSeatModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                <button type="submit" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Create Seat</button>
            </div>
        </form>
    </div>
</div>"""

if old_add in text:
    text = text.replace(old_add, new_add)
    print("Replaced Add Seat modal")
else:
    print("Could not find Add Seat modal")

old_photo = """            <div class="pt-4 border-t border-slate-100 text-right">
                <button type="submit" class="ui-btn ui-btn-primary w-full">Save Photo</button>
            </div>"""

new_photo = """            <div class="pt-4 border-t border-slate-100 flex justify-end space-x-3">
                <button type="button" onclick="document.getElementById('photoModal').classList.add('hidden')" class="px-4 py-2 bg-white border border-slate-200 text-slate-600 font-bold rounded-lg shadow-sm hover:bg-slate-50">Cancel</button>
                <button type="submit" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Save Photo</button>
            </div>"""

if old_photo in text:
    text = text.replace(old_photo, new_photo)
    print("Replaced Photo modal buttons")
else:
    print("Could not find Photo modal buttons")

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
