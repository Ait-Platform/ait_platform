import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace macro calls with a tabbed interface
old_tables = '''      {{ job_table("Pending Quotes", pending, "amber", "table-pending") }}
      {{ job_table("Accepted / In Progress", accepted, "blue", "table-accepted") }}
      {{ job_table("Completed / Billed", completed, "green", "table-completed") }}
      {{ job_table("Rejected Quotes", rejected, "slate", "table-rejected") }}

    </div>
  </div>
</div>

<script>'''

new_tables = '''
      <!-- Tabs Navigation -->
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
      </div>

      <div id="tab-content-pending" class="tab-pane block">
        {{ job_table("Pending Quotes", pending, "amber", "table-pending") }}
      </div>
      <div id="tab-content-accepted" class="tab-pane hidden">
        {{ job_table("Accepted / In Progress", accepted, "blue", "table-accepted") }}
      </div>
      <div id="tab-content-completed" class="tab-pane hidden">
        {{ job_table("Completed / Billed", completed, "green", "table-completed") }}
      </div>
      <div id="tab-content-rejected" class="tab-pane hidden">
        {{ job_table("Rejected Quotes", rejected, "slate", "table-rejected") }}
      </div>

    </div>
  </div>
</div>

<script>
function switchTab(tabId) {
    // Hide all tab content
    document.querySelectorAll('.tab-pane').forEach(el => {
        el.classList.remove('block');
        el.classList.add('hidden');
    });
    
    // Show selected tab content
    const selectedPane = document.getElementById('tab-content-' + tabId);
    if(selectedPane) {
        selectedPane.classList.remove('hidden');
        selectedPane.classList.add('block');
    }
    
    // Reset all buttons
    const tabs = ['pending', 'accepted', 'completed', 'rejected'];
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
    }
}
'''

content = content.replace(old_tables, new_tables)
with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
