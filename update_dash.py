import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# We will completely replace the grid contents for the if active_shop block.
# We need to find the start of the if active_shop inside the grid.
grid_start_marker = '''        {% if not active_shop %}'''
grid_end_marker = '''        <!-- Help Tile -->'''

# Actually, it's easier to replace the entire grid div.
new_grid = '''      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
        
        {% if not active_shop %}
          <!-- User has no shop set up -->
          <button type="button" onclick="document.getElementById('manual-setup-modal').classList.remove('hidden')" class="block text-left rounded-xl border p-6 shadow-sm transition hover:shadow bg-white border-slate-200 hover:border-indigo-300 group">
            <div class="font-semibold text-slate-900 group-hover:text-indigo-600">1. Complete Shop Setup</div>
            <div class="mt-1 text-sm text-slate-600">Enter your business name, address, and logo manually.</div>
          </button>
          
          <div class="block rounded-xl border-2 p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-bold text-slate-900 text-lg">Create Quote</div>
            <div class="mt-1 text-sm text-slate-700">Requires an active Shop Profile to start quoting.</div>
          </div>
        {% else %}
          
          <!-- LEFT: Quote & Jobs Tile Block -->
          <div class="flex flex-col gap-2">
            <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
              <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
            </a>
            <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block text-center w-full rounded-xl border-2 border-indigo-200 p-3 shadow-sm transition hover:shadow bg-indigo-50 hover:border-indigo-400 text-indigo-900 font-semibold text-sm group">
              <i class="fas fa-list-alt mr-1 group-hover:text-indigo-700"></i> Recent Job Cards
            </a>
          </div>

          <!-- MIDDLE: Catalog & Debtors Block -->
          <div class="flex flex-col gap-2">
            <a href="{{ url_for('mechanic_bp.catalog_manage') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-purple-50 border-purple-200 hover:border-purple-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-purple-700 text-lg">Manage Catalog</div>
              <div class="mt-1 text-sm text-slate-700">Add custom parts and set your local prices.</div>
            </a>
            <a href="{{ url_for('debtors_bp.dashboard') }}" class="block text-center w-full rounded-xl border-2 border-sky-200 p-3 shadow-sm transition hover:shadow bg-sky-50 hover:border-sky-400 text-sky-900 font-semibold text-sm group">
              <i class="fas fa-file-invoice-dollar mr-1 group-hover:text-sky-700"></i> Debtors (SOA)
            </a>
          </div>

          <!-- RIGHT: Shop Profile & Preview Block -->
          <div class="flex flex-col gap-2">
            <button type="button" onclick="document.getElementById('manual-setup-modal').classList.remove('hidden')" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-blue-50 border-blue-200 hover:border-blue-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-blue-700 text-lg">Update Shop Profile</div>
              <div class="mt-1 text-sm text-slate-700">Edit business details, terms, and logo.</div>
            </button>
            <a href="{{ url_for('mechanic_bp.document_preview') }}" class="block text-center w-full rounded-xl border-2 border-teal-200 p-3 shadow-sm transition hover:shadow bg-teal-50 hover:border-teal-400 text-teal-900 font-semibold text-sm group">
              <i class="fas fa-file-pdf mr-1 group-hover:text-teal-700"></i> Preview Formatted Document
            </a>
          </div>

        {% endif %}

        <!-- Row 2 -->
        
        <!-- Wallet & Transfer Tile Block -->
        <div class="flex flex-col gap-2">
          <a href="{{ url_for('mechanic_bp.price_page') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group flex-1">
            <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Wallet</div>
            <div class="mt-1 text-base font-bold text-teal-600 mb-1">{{ wallet.balance if wallet else 0 }} Tokens</div>
            <div class="text-sm text-slate-700">Topup & Manage</div>
          </a>
          <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block text-center w-full rounded-xl border-2 border-orange-200 p-3 shadow-sm transition hover:shadow bg-orange-50 hover:border-orange-400 text-orange-900 font-semibold text-sm group">
            <i class="fas fa-exchange-alt mr-1 group-hover:text-orange-700"></i> Transfer Tokens
          </a>
        </div>
        
        {% if active_shop %}
          <a href="{{ url_for('mechanic_bp.communication_logs') }}" class="block text-left rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-pink-50 border-pink-200 hover:border-pink-400 group h-full">
            <div class="font-bold text-slate-900 group-hover:text-pink-700 text-lg">Communication Logs</div>
            <div class="mt-1 text-sm text-slate-700">View history of WhatsApp reminders and invites.</div>
          </a>
        {% endif %}

      </div>'''

# Replace the entire grid
grid_regex = re.compile(r'<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">.*?(?=    </div>\n\n\n<script>)', re.DOTALL)
content = grid_regex.sub(new_grid, content)

# Universal Help FAB
fab = '''
<!-- Universal Help FAB -->
<a href="{{ url_for('mechanic_bp.help_center') }}" class="fixed bottom-6 right-6 w-14 h-14 bg-indigo-600 hover:bg-indigo-700 text-white rounded-full shadow-lg flex items-center justify-center transition transform hover:scale-110 z-50 group">
  <i class="fas fa-question text-2xl"></i>
  <span class="absolute right-16 bg-slate-800 text-white text-xs font-bold px-3 py-1.5 rounded opacity-0 group-hover:opacity-100 transition whitespace-nowrap shadow-md pointer-events-none">Help Center</span>
</a>
'''

content = content.replace('{% endblock %}', fab + '\n{% endblock %}')

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated dashboard.html layout")
