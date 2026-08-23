import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

new_buttons = '''        <div class="flex flex-wrap gap-3 mt-4 mb-8">
          <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Pending</span>
            <span class="bg-white text-indigo-700 py-0.5 px-2 rounded-full font-black">{{ pending|length }}</span>
          </button>
          <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Confirmed</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ accepted|length }}</span>
          </button>
          <button onclick="switchTab('completed')" id="tab-btn-completed" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Completed</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ completed|length }}</span>
          </button>
          <a href="{{ url_for('debtors_bp.debtors_dashboard') }}" id="tab-btn-ledger" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px] ml-4">
            <span class="mb-1">Ledger</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ total_debtors_count }}</span>
          </a>
        </div>'''

regex = r'<div class="flex flex-wrap gap-3 mt-4 mb-8">.*?</div>'
content = re.sub(regex, new_buttons, content, flags=re.DOTALL)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
