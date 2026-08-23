import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_tabs = '''      <!-- Tabs Navigation -->
      <div class="border-b border-slate-200 mt-8 mb-6">
        <nav class="-mb-px flex space-x-8" aria-label="Tabs">
          <button onclick="switchTab('pending')" id="tab-btn-pending" class="border-indigo-500 text-indigo-600 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm">
            Pending Quotes <span class="bg-indigo-100 text-indigo-600 py-0.5 px-2 rounded-full text-xs ml-1">{{ pending|length }}</span>
          </button>
          <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm">
            Accepted / In Progress <span class="bg-slate-100 text-slate-600 py-0.5 px-2 rounded-full text-xs ml-1">{{ accepted|length }}</span>
          </button>
          <button onclick="switchTab('completed')" id="tab-btn-completed" class="border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm">
            Completed / Billed <span class="bg-slate-100 text-slate-600 py-0.5 px-2 rounded-full text-xs ml-1">{{ completed|length }}</span>
          </button>
          <button onclick="switchTab('rejected')" id="tab-btn-rejected" class="border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm">
            Rejected <span class="bg-slate-100 text-slate-600 py-0.5 px-2 rounded-full text-xs ml-1">{{ rejected|length }}</span>
          </button>
        </nav>
      </div>'''

new_tabs = '''      <!-- Tabs Navigation (Styled as Buttons) -->
      <div class="flex flex-wrap gap-3 mt-4 mb-8">
        <button onclick="switchTab('pending')" id="tab-btn-pending" class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700">
          Pending Quotes <span class="bg-white text-indigo-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ pending|length }}</span>
        </button>
        <button onclick="switchTab('accepted')" id="tab-btn-accepted" class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Accepted / In Progress <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ accepted|length }}</span>
        </button>
        <button onclick="switchTab('completed')" id="tab-btn-completed" class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Completed / Billed <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ completed|length }}</span>
        </button>
        <button onclick="switchTab('rejected')" id="tab-btn-rejected" class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200">
          Rejected <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1">{{ rejected|length }}</span>
        </button>
      </div>'''

content = content.replace(old_tabs, new_tabs)

# Fix script function
old_script = '''    const tabs = ['pending', 'accepted', 'completed', 'rejected'];
    tabs.forEach(t => {
        const btn = document.getElementById('tab-btn-' + t);
        if(btn) {
            btn.className = "border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm";
            // Update badge color
            const badge = btn.querySelector('span');
            if (badge) badge.className = "bg-slate-100 text-slate-600 py-0.5 px-2 rounded-full text-xs ml-1";
        }
    });
    
    // Highlight active button
    const activeBtn = document.getElementById('tab-btn-' + tabId);
    if(activeBtn) {
        activeBtn.className = "border-indigo-500 text-indigo-600 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm";
        const badge = activeBtn.querySelector('span');
        if (badge) badge.className = "bg-indigo-100 text-indigo-600 py-0.5 px-2 rounded-full text-xs ml-1";
    }'''

new_script = '''    const tabs = ['pending', 'accepted', 'completed', 'rejected'];
    tabs.forEach(t => {
        const btn = document.getElementById('tab-btn-' + t);
        if(btn) {
            btn.className = "flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200";
            // Update badge color
            const badge = btn.querySelector('span');
            if (badge) badge.className = "bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1";
        }
    });
    
    // Highlight active button
    const activeBtn = document.getElementById('tab-btn-' + tabId);
    if(activeBtn) {
        activeBtn.className = "flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700";
        const badge = activeBtn.querySelector('span');
        if (badge) badge.className = "bg-white text-indigo-700 py-0.5 px-2 rounded-full text-xs ml-1";
    }'''

content = content.replace(old_script, new_script)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
