import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_block = '''<!-- Tabs Navigation (Styled as Buttons) -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6 mb-8 bg-slate-50 p-4 rounded-xl border border-slate-200 shadow-inner">
            <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center w-full">
              <span class="mb-2 uppercase tracking-wider text-xs">Pending</span>
              <span class="bg-white text-indigo-700 py-1 px-3 rounded-full font-black text-lg">{{ pending|length }}</span>
            </button>
            <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full">
              <span class="mb-2 uppercase tracking-wider text-xs">Confirmed</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200">{{ accepted|length }}</span>
            </button>
            <a href="{{ url_for('mechanic_bp.client_accounts') }}" id="tab-btn-ledger" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full group">
              <span class="mb-2 uppercase tracking-wider text-xs flex items-center gap-2"><i class="fas fa-file-invoice-dollar text-slate-400 group-hover:text-emerald-500 transition"></i> Ledger</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200">{{ total_debtors_count }}</span>
            </a>
            <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full group">
              <span class="mb-2 uppercase tracking-wider text-xs flex items-center gap-2"><i class="fas fa-search text-slate-400 group-hover:text-blue-500 transition"></i> Tracker</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200"><i class="fas fa-car-side"></i></span>
            </button>
          </div>'''

new_block = '''<!-- Tabs Navigation (Styled as Buttons) -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6 mb-8 bg-slate-200 p-4 rounded-xl border-2 border-slate-300 shadow-inner">
            <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center w-full">
              <span class="mb-2 uppercase tracking-wider text-xs">Pending</span>
              <span class="bg-white text-indigo-700 py-1 px-3 rounded-full font-black text-lg">{{ pending|length }}</span>
            </button>
            <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full">
              <span class="mb-2 uppercase tracking-wider text-xs">Confirmed</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200">{{ accepted|length }}</span>
            </button>
            <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full group">
              <span class="mb-2 uppercase tracking-wider text-xs flex items-center gap-2"><i class="fas fa-search text-slate-400 group-hover:text-blue-500 transition"></i> Tracker</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200"><i class="fas fa-car-side"></i></span>
            </button>
            <a href="{{ url_for('mechanic_bp.client_accounts') }}" id="tab-btn-ledger" class="flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full group">
              <span class="mb-2 uppercase tracking-wider text-xs flex items-center gap-2"><i class="fas fa-file-invoice-dollar text-slate-400 group-hover:text-emerald-500 transition"></i> Ledger</span>
              <span class="bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200">{{ total_debtors_count }}</span>
            </a>
          </div>'''

content = content.replace(old_block, new_block)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
