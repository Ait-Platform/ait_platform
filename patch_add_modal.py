with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

old_html = """              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Group Level</label>
                  <select name="group_level" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg 
focus:ring-2 focus:ring-indigo-500">"""

new_html = """              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Platform Permission (Duty)</label>
                  <select name="duty" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 mb-4">
                      <option value="manager">Manager (Full Access)</option>
                      <option value="committee_member" selected>Committee Member (Standard)</option>
                      <option value="receptionist">Receptionist (Intake Desk)</option>
                  </select>
                  <p class="text-[10px] text-slate-500 -mt-3 mb-2">The system keys this seat will automatically grant.</p>
              </div>
              
              <div>
                  <label class="block text-xs font-bold text-slate-700 mb-1">Group Level</label>
                  <select name="group_level" required class="w-full p-2.5 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">"""

text = text.replace(old_html.replace('\n', '\n'), new_html)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated addSeatModal HTML")
