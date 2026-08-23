import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace button HTML
old_buttons = '''        <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700">
          Pending Quotes <span class="bg-white text-indigo-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ pending|length }}</span>
        </button>
        <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Accepted / In Progress <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ accepted|length }}</span>
        </button>
        <button onclick="switchTab('completed')" id="tab-btn-completed" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Completed / Billed <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ completed|length }}</span>
        </button>
        <button onclick="switchTab('rejected')" id="tab-btn-rejected" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Rejected <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ rejected|length }}</span>
        </button>'''

new_buttons = '''        <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center leading-tight min-w-[80px]">
          <span class="mb-1">Pending<br>Quotes</span>
          <span class="bg-white text-indigo-700 py-0.5 px-2 rounded-full font-black">{{ pending|length }}</span>
        </button>
        <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
          <span class="mb-1">Accepted /<br>In Progress</span>
          <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ accepted|length }}</span>
        </button>
        <button onclick="switchTab('completed')" id="tab-btn-completed" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
          <span class="mb-1">Completed /<br>Billed</span>
          <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ completed|length }}</span>
        </button>
        <button onclick="switchTab('rejected')" id="tab-btn-rejected" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
          <span class="mb-1">Rejected<br>Quotes</span>
          <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ rejected|length }}</span>
        </button>'''

content = content.replace(old_buttons, new_buttons)

# Also fix the JS that resets the classes
old_js_active = 'activeBtn.className = "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700";'
new_js_active = 'activeBtn.className = "flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center leading-tight min-w-[80px]";'
content = content.replace(old_js_active, new_js_active)

old_js_inactive = 'btn.className = "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200";'
new_js_inactive = 'btn.className = "flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]";'
content = content.replace(old_js_inactive, new_js_inactive)

# Also, JS overrides the badge classes, we need to update that too
old_js_active_badge = 'badge.className = "bg-white text-indigo-700 py-0.5 px-2 rounded-full text-xs ml-1";'
new_js_active_badge = 'badge.className = "bg-white text-indigo-700 py-0.5 px-2 rounded-full font-black";'
content = content.replace(old_js_active_badge, new_js_active_badge)

old_js_inactive_badge = 'b.className = "bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1";'
new_js_inactive_badge = 'b.className = "bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black";'
content = content.replace(old_js_inactive_badge, new_js_inactive_badge)


with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
