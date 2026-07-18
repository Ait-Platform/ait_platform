with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Insert Status Dropdown into HTML template
old_html = """        <div>
          <label class="block text-xs font-semibold mb-1 text-slate-700">Type / Assignment</label>
          <select class="meter-assignment w-full rounded border-2 border-slate-300 px-2 py-1.5 outline-none focus:border-emerald-500 transition text-sm bg-white">
            <option value="linked">Linked (Sub-meter)</option>
            <option value="bulk">Bulk (Main)</option>
          </select>
        </div>"""

new_html = """        <div>
          <label class="block text-xs font-semibold mb-1 text-slate-700">Type / Assignment</label>
          <select class="meter-assignment w-full rounded border-2 border-slate-300 px-2 py-1.5 outline-none focus:border-emerald-500 transition text-sm bg-white">
            <option value="linked">Linked (Sub-meter)</option>
            <option value="bulk">Bulk (Main)</option>
          </select>
        </div>
        <div class="col-span-2">
          <label class="block text-xs font-semibold mb-1 text-slate-700">Meter Status</label>
          <select class="meter-status w-full rounded border-2 border-slate-300 px-2 py-1.5 outline-none focus:border-emerald-500 transition text-sm bg-white">
            <option value="active">Active & Normal (On Bill + Physical)</option>
            <option value="stolen">Stolen / Defective (On Bill Only)</option>
            <option value="new_physical">New / Replacement (Physical Only)</option>
          </select>
        </div>"""

if old_html in content:
    content = content.replace(old_html, new_html)

# Insert status into JS payload
old_js = "assignment: card.querySelector('.meter-assignment').value,"
new_js = """assignment: card.querySelector('.meter-assignment').value,
        status: card.querySelector('.meter-status').value,"""

if old_js in content:
    content = content.replace(old_js, new_js)

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated manual_capture.html")
