import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Instead of regex, let's use string splitting and replacement since we know the exact blocks
comm_log_block = '''          <a href="{{ url_for('mechanic_bp.communication_logs') }}" class="block text-left rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-pink-50 border-pink-200 hover:border-pink-400 group">
            <div class="font-bold text-slate-900 group-hover:text-pink-700 text-lg">Communication Logs</div>
            <div class="mt-1 text-sm text-slate-700">View history of WhatsApp reminders and invites.</div>
          </a>'''

quote_block = '''        {% if active_shop %}
          <a href="{{ url_for('mechanic_bp.new_quote') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-green-50 border-green-200 hover:border-green-400 group">
            <div class="font-bold text-slate-900 group-hover:text-green-700 text-lg">Create Quote</div>
            <div class="mt-1 text-sm text-slate-700">Add Customer & Select Parts for a new quote.</div>
          </a>
        {% else %}
          <div class="block rounded-xl border-2 p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-bold text-slate-900 text-lg">Create Quote</div>
            <div class="mt-1 text-sm text-slate-700">Requires an active Shop Profile to start quoting.</div>
          </div>
        {% endif %}'''

# Remove them from current positions
content = content.replace(comm_log_block, "")
content = content.replace(quote_block, "")

# Insert Quote Block where comm_log was (after Manage Catalog)
catalog_block = '''          <a href="{{ url_for('mechanic_bp.catalog_manage') }}" class="block text-left rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-purple-50 border-purple-200 hover:border-purple-400 group">
            <div class="font-bold text-slate-900 group-hover:text-purple-700 text-lg">Manage Catalog</div>
            <div class="mt-1 text-sm text-slate-700">Add custom parts and set your local prices.</div>
          </a>'''
content = content.replace(catalog_block, catalog_block + "\n\n" + quote_block)

# Insert Comm Log where Quote Block was (after Transfer Tokens)
transfer_block = '''        <!-- Transfer Tokens Tile -->
        <a href="{{ url_for('cultural_bp.wallet_transfer_page') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-orange-50 border-orange-200 hover:border-orange-400 group">
          <div class="font-bold text-slate-900 group-hover:text-orange-700 text-lg">Transfer Tokens</div>
          <div class="mt-1 text-sm text-slate-700">Send & Generate Vouchers</div>
        </a>'''

jobs_tile = '''
        <!-- Recent Job Cards Tile -->
        <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="block rounded-xl border-2 p-6 shadow-sm transition hover:shadow-md bg-indigo-50 border-indigo-200 hover:border-indigo-400 group">
          <div class="font-bold text-slate-900 group-hover:text-indigo-700 text-lg">Recent Job Cards</div>
          <div class="mt-1 text-sm text-slate-700">View and manage all active quotes and billed jobs.</div>
        </a>
'''
content = content.replace(transfer_block, transfer_block + "\n\n" + comm_log_block + jobs_tile)

# Remove the table
table_regex = r'(<!-- Job Cards Table -->.*?</script>)'
content = re.sub(table_regex, "", content, flags=re.DOTALL)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated dashboard!")
