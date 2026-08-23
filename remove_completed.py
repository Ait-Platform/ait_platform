import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove Button
btn = '''          <button onclick="switchTab('completed')" id="tab-btn-completed" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Completed</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ completed|length }}</span>
          </button>
'''
content = content.replace(btn, '')

# 2. Remove Pane
pane = '''      <div id="tab-content-completed" class="tab-pane hidden">
        {{ job_table("Completed / Billed", completed, "green", "table-completed") }}
      </div>
'''
content = content.replace(pane, '')

# 3. Update JS array
content = content.replace("const tabs = ['pending', 'accepted', 'completed', 'tracker', 'rejected'];", "const tabs = ['pending', 'accepted', 'tracker', 'rejected'];")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
