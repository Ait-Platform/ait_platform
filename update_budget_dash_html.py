import re

with open('templates/program_budget/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      <div class="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
        
        <!-- Wallet & Transfer Tile Block -->
        <div class="flex flex-col gap-2">
            <a href="{{ url_for('budget_bp.price_page') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group flex-1">
                <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Wallet</div>
                <div class="mt-1 text-base font-bold text-teal-600 mb-1">{{ wallet.balance if wallet else 0 }} Tokens</div>
                <div class="text-sm text-slate-700">Topup & Manage</div>
            </a>
            <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block text-center w-full rounded-xl border-2 border-orange-200 p-3 shadow-sm transition hover:shadow bg-orange-50 hover:border-orange-400 text-orange-900 font-semibold text-sm group">
                <i class="fas fa-exchange-alt mr-1 group-hover:text-orange-700"></i> Transfer Tokens
            </a>
        </div>
        
        <!--div class="grid grid-cols-1 sm:grid-cols-3 gap-3"-->'''

content = content.replace('      <!-- Row 1 -->\n      <div class="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">\n        <!--div class="grid grid-cols-1 sm:grid-cols-3 gap-3"-->', replacement)

with open('templates/program_budget/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
