import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# The original file had a massive {% if not active_shop %} block for the tiles.
# Let's clean it up by replacing the entire tiles section to ensure perfectly balanced Jinja tags.

start_marker = '<div class="mb-8 w-full px-6">'
end_marker = '<script>'

new_tiles_block = '''<div class="mb-8 w-full px-6">
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
        
        <!-- AI disabled temporarily. Manual Setup Tile -->
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
          <!-- User has shop set up -->
          <div class="flex flex-col gap-2">
            <button type="button" onclick="document.getElementById('manual-setup-modal').classList.remove('hidden')" class="block text-left w-full rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-blue-50 border-blue-200 hover:border-blue-400 group">
              <div class="font-bold text-slate-900 group-hover:text-blue-700 text-lg">Update Shop Profile</div>
              <div class="mt-1 text-sm text-slate-700">Edit business details, terms, and logo.</div>
            </button>
            <a href="{{ url_for('mechanic_bp.document_preview') }}" class="block text-center w-full rounded-xl border-2 border-teal-200 p-3 shadow-sm transition hover:shadow bg-teal-50 hover:border-teal-400 text-teal-900 font-semibold text-sm group">
              <i class="fas fa-file-pdf mr-1 group-hover:text-teal-700"></i> Preview Blank Document
            </a>
          </div>

          <a href="{{ url_for('mechanic_bp.catalog_manage') }}" class="block text-left rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-purple-50 border-purple-200 hover:border-purple-400 group">
            <div class="font-bold text-slate-900 group-hover:text-purple-700 text-lg">Manage Catalog</div>
            <div class="mt-1 text-sm text-slate-700">Add custom parts and set your local prices.</div>
          </a>

          <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group">
            <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
            <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
          </a>
        {% endif %}

        <!-- Wallet Tile -->
        <a href="{{ url_for('mechanic_bp.price_page') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group">
          <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Wallet</div>
          <div class="mt-1 text-base font-bold text-teal-600 mb-1">{{ wallet.balance if wallet else 0 }} Tokens</div>
          <div class="text-sm text-slate-700">Topup & Manage</div>
        </a>

        <!-- Transfer Tokens Tile -->
        <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-orange-50 border-orange-200 hover:border-orange-400 group">
          <div class="font-bold text-slate-900 group-hover:text-orange-700 text-lg">Transfer Tokens</div>
          <div class="mt-1 text-sm text-slate-700">Send & Generate Vouchers</div>
        </a>
        
        {% if active_shop %}
          <a href="{{ url_for('mechanic_bp.communication_logs') }}" class="block text-left rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-pink-50 border-pink-200 hover:border-pink-400 group">
            <div class="font-bold text-slate-900 group-hover:text-pink-700 text-lg">Communication Logs</div>
            <div class="mt-1 text-sm text-slate-700">View history of WhatsApp reminders and invites.</div>
          </a>
        {% endif %}

        <!-- Recent Job Cards Tile -->
        <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-indigo-50 border-indigo-200 hover:border-indigo-400 group">
          <div class="font-bold text-slate-900 group-hover:text-indigo-700 text-lg">Recent Job Cards</div>
          <div class="mt-1 text-sm text-slate-700">View and manage all active quotes and billed jobs.</div>
        </a>

        <!-- Debtors SOA Tile -->
        <a href="{{ url_for('debtors_bp.dashboard') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-sky-50 border-sky-200 hover:border-sky-400 group">
          <div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Debtors (SOA)</div>
          <div class="mt-1 text-sm text-slate-700">Manage client accounts, invoices, and payments.</div>
        </a>

        <!-- Help Tile -->
        <a href="{{ url_for('mechanic_bp.help_center') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-amber-50 border-amber-200 hover:border-amber-400 group">
          <div class="font-bold text-slate-900 group-hover:text-amber-700 text-lg">Help Center</div>
          <div class="mt-1 text-sm text-slate-700">Guides for ProTrade, Quotes, and Debtors tracking.</div>
        </a>
      </div>
    </div>
'''

content_to_replace = content[content.find(start_marker) : content.find(end_marker)]
content = content.replace(content_to_replace, new_tiles_block + "\n\n")

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated dashboard.html")
