with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

replacement = '''      <div class="mb-8 w-full flex flex-col md:flex-row md:justify-between md:items-start">
        <div>
          <h1 class="text-3xl font-extrabold text-slate-800 tracking-tight">Billing Dashboard</h1>
          <p class="text-slate-500 mt-2">Follow the tile sequence below to onboard a new property.</p>
        </div>
      </div>

      <div class="mb-8 w-full">
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
          
          <!-- Tile 1: Setup Properties -->
          {% if draft_property %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">1. Setup Properties</div>
            <div class="mt-1 text-sm text-slate-600">Locked. You must finish setting up '{{ draft_property.name }}' first.</div>
          </div>
          {% else %}
          <button onclick="document.getElementById('setupModal').classList.remove('hidden')" class="text-left block rounded-xl border p-6 shadow-sm transition hover:shadow bg-blue-50 border-blue-100 hover:bg-blue-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-blue-700">1. Setup Properties</div>
            <div class="mt-1 text-sm text-slate-600">Answer 3 questions to initiate a new property setup.</div>
          </button>
          {% endif %}
          
          <!-- Tile 2: View Extraction -->
          {% if draft_property and draft_property.onboarding_status in ['draft_extracting', 'draft_collating'] %}
          <a href="{{ url_for('billing_bp.ai_onboarding') }}?property_id={{ draft_property.id }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-indigo-50 border-indigo-100 hover:bg-indigo-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-indigo-700">2. View Extraction</div>
            <div class="mt-1 text-sm text-slate-600">Upload bills. ({{ uploaded_bills }} / {{ draft_property.expected_bills }} uploaded).</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">2. View Extraction</div>
            <div class="mt-1 text-sm text-slate-600">Locked until a property setup is initiated.</div>
          </div>
          {% endif %}

          <!-- Tile 3: Collation and Editing -->
          {% if draft_property and draft_property.onboarding_status == 'draft_collating' %}
          <a href="{{ url_for('billing_bp.ai_onboarding') }}?property_id={{ draft_property.id }}&view=collation" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-purple-50 border-purple-100 hover:bg-purple-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-purple-700">3. Collation and Editing</div>
            <div class="mt-1 text-sm text-slate-600">Map your uploaded meters to your tenants.</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">3. Collation and Editing</div>
            <div class="mt-1 text-sm text-slate-600">Locked until all expected bills are extracted.</div>
          </div>
          {% endif %}

          <!-- Tile 4: Enter Readings (MetSOA) -->
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">4. Enter Readings</div>
            <div class="mt-1 text-sm text-slate-600">Locked until collation is complete.</div>
          </div>

          <!-- Tile 5: Financial Requirements -->
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">5. Financial Requirements</div>
            <div class="mt-1 text-sm text-slate-600">Locked until readings are finalized.</div>
          </div>

          <!-- Tile 6: Help -->
          <a href="#" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-emerald-50 border-emerald-100 hover:bg-emerald-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-emerald-700">Help / Guide</div>
            <div class="mt-1 text-sm text-slate-600">Learn how our unique bill combination system works.</div>
          </a>

        </div>
      </div>

      <!-- Setup Modal -->
      <div id="setupModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-slate-900 bg-opacity-50">
        <div class="bg-white rounded-xl shadow-xl w-full max-w-lg overflow-hidden relative">
          <div class="px-6 py-4 border-b border-slate-200 bg-slate-50 flex justify-between items-center">
            <h3 class="font-bold text-slate-800 text-lg">Setup Property Details</h3>
            <button onclick="document.getElementById('setupModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 text-2xl leading-none">&times;</button>
          </div>
          <form action="/billing/onboarding/start_setup" method="POST" class="p-6">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Property Name</label>
              <input type="text" name="property_name" required class="w-full border border-slate-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">How many distinct physical bills do you expect to upload?</label>
              <input type="number" name="bills" min="1" value="1" required class="w-full border border-slate-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">How many tenants/statements are required?</label>
              <input type="number" name="tenants" min="1" value="1" required class="w-full border border-slate-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Is this a Bulk Metered property?</label>
              <select name="is_bulk" class="w-full border border-slate-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
                <option value="no">No, just standard individual meters</option>
                <option value="yes">Yes, it has a main bulk meter and sub-meters</option>
              </select>
            </div>
            <div class="mb-6">
              <label class="block text-sm font-bold text-slate-700 mb-2">If Bulk, how many sub-meters are linked?</label>
              <input type="number" name="sub_meters" min="0" value="0" class="w-full border border-slate-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="flex justify-end">
              <button type="submit" class="px-6 py-2 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700 transition">Initialize Setup</button>
            </div>
          </form>
        </div>
      </div>
'''

new_lines = lines[:10] + [replacement] + lines[158:]

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
