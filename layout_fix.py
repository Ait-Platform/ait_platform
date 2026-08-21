import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Rename Preview Blank Document -> Preview Formatted Document
content = content.replace("Preview Blank Document", "Preview Formatted Document")

# 2. Combine Wallet and Transfer Tokens
wallet_transfer_original = '''        <!-- Wallet Tile -->
        <a href="{{ url_for('mechanic_bp.price_page') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group">
          <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Wallet</div>
          <div class="mt-1 text-base font-bold text-teal-600 mb-1">{{ wallet.balance if wallet else 0 }} Tokens</div>
          <div class="text-sm text-slate-700">Topup & Manage</div>
        </a>

        <!-- Transfer Tokens Tile -->
        <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-orange-50 border-orange-200 hover:border-orange-400 group">
          <div class="font-bold text-slate-900 group-hover:text-orange-700 text-lg">Transfer Tokens</div>
          <div class="mt-1 text-sm text-slate-700">Send & Generate Vouchers</div>
        </a>'''

wallet_transfer_new = '''        <!-- Wallet & Transfer Tile Block -->
        <div class="flex flex-col gap-2">
          <a href="{{ url_for('mechanic_bp.price_page') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group flex-1">
            <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Wallet</div>
            <div class="mt-1 text-base font-bold text-teal-600 mb-1">{{ wallet.balance if wallet else 0 }} Tokens</div>
            <div class="text-sm text-slate-700">Topup & Manage</div>
          </a>
          <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block text-center w-full rounded-xl border-2 border-orange-200 p-3 shadow-sm transition hover:shadow bg-orange-50 hover:border-orange-400 text-orange-900 font-semibold text-sm group">
            <i class="fas fa-exchange-alt mr-1 group-hover:text-orange-700"></i> Transfer Tokens
          </a>
        </div>'''

if wallet_transfer_original in content:
    content = content.replace(wallet_transfer_original, wallet_transfer_new)
else:
    print("Could not find Wallet/Transfer block")

# 3. Combine Create Quote and Recent Job Cards
quote_original = '''          <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group">
            <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
            <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
          </a>'''

jobs_original = '''        <!-- Recent Job Cards Tile -->
        <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-indigo-50 border-indigo-200 hover:border-indigo-400 group">
          <div class="font-bold text-slate-900 group-hover:text-indigo-700 text-lg">Recent Job Cards</div>
          <div class="mt-1 text-sm text-slate-700">View and manage all active quotes and billed jobs.</div>
        </a>'''

combined_quote_jobs = '''          <!-- Quote & Jobs Tile Block -->
          <div class="flex flex-col gap-2">
            <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
              <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
            </a>
            <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block text-center w-full rounded-xl border-2 border-indigo-200 p-3 shadow-sm transition hover:shadow bg-indigo-50 hover:border-indigo-400 text-indigo-900 font-semibold text-sm group">
              <i class="fas fa-list-alt mr-1 group-hover:text-indigo-700"></i> Recent Job Cards
            </a>
          </div>'''

# Replace the original quote tile with the combined block
if quote_original in content:
    content = content.replace(quote_original, combined_quote_jobs)
    # Then remove the old isolated jobs tile
    if jobs_original in content:
        content = content.replace(jobs_original, "")
else:
    print("Could not find Create Quote block")


with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated dashboard.html successfully without breaking Jinja!")
