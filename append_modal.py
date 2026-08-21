with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

modal_code = '''  <!-- Manual Setup Modal -->
  <div id="manual-setup-modal" class="fixed inset-0 z-50 hidden flex items-center justify-center bg-slate-900 bg-opacity-50 p-4 sm:p-6" aria-labelledby="modal-title" role="dialog" aria-modal="true">
    <div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-y-auto max-h-[90vh] relative">
      <div class="px-8 py-5 border-b border-slate-200 bg-slate-50 flex justify-between items-center rounded-t-2xl">
        <h3 class="font-extrabold text-slate-800 text-xl">Shop Profile Setup</h3>
        <button onclick="document.getElementById('manual-setup-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 text-3xl leading-none">&times;</button>
      </div>
      <form action="{{ url_for('mechanic_bp.onboarding_process') }}" method="POST" enctype="multipart/form-data">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        <div class="p-8 space-y-6">
          <p class="text-base text-slate-600 mb-2">Please enter your business details to configure your quotes and invoices.</p>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
            <div class="sm:col-span-2">
              <label class="block text-sm font-bold text-slate-700 mb-1">Business Name</label>
              <input type="text" name="business_name" value="{{ active_shop.business_name if active_shop else '' }}" required autofocus class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
            </div>
            <div class="sm:col-span-2">
              <label class="block text-sm font-bold text-slate-700 mb-1">Address</label>
              <input type="text" name="address" value="{{ active_shop.address if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
              <input type="text" name="phone" value="{{ active_shop.phone if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
              <input type="email" name="email" value="{{ active_shop.email if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
            </div>
            <div class="sm:col-span-2">
              <label class="block text-sm font-bold text-slate-700 mb-1">Shop Logo (Optional)</label>
              {% if active_shop and active_shop.logo_url %}
              <div class="mb-3">
                <img src="{{ url_for('static', filename='uploads/mechanic/' + active_shop.logo_url) }}" alt="Shop Logo" class="h-16 w-auto object-contain rounded border border-slate-200 shadow-sm bg-white p-1">
              </div>
              {% endif %}
              <input type="file" name="logo_file" accept="image/*" class="block w-full text-base text-slate-600 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-bold file:bg-indigo-100 file:text-indigo-700 hover:file:bg-indigo-200 transition cursor-pointer border-2 border-dashed border-indigo-200 p-4 rounded-lg">
              
              <div class="mt-6 border-t border-slate-200 pt-4">
                <label class="block text-sm font-bold text-slate-700 mb-1">Custom Letterhead Banner (Optional)</label>
                <p class="text-xs text-slate-500 mb-2">Upload a full-width image to replace the standard invoice header.</p>
                {% if active_shop and active_shop.letterhead_url %}
                <div class="mb-3">
                  <img src="{{ url_for('static', filename='uploads/mechanic/' + active_shop.letterhead_url) }}" alt="Shop Letterhead" class="h-16 w-full object-cover rounded border border-slate-200 shadow-sm bg-white">
                </div>
                {% endif %}
                <input type="file" name="letterhead_file" accept="image/*" class="block w-full text-base text-slate-600 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-bold file:bg-indigo-100 file:text-indigo-700 hover:file:bg-indigo-200 transition cursor-pointer border-2 border-dashed border-indigo-200 p-4 rounded-lg">
              </div>

              <div class="mt-6 border-t border-slate-200 pt-4">
                <label class="block text-sm font-bold text-slate-700 mb-1">Terms & Conditions (Optional)</label>
                <p class="text-xs text-slate-500 mb-2">These will appear at the bottom of your Quotes and Invoices.</p>
                <div class="mb-2">
                  <select id="tc-template-select" onchange="insertTCTemplate()" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-2 transition mb-2 bg-slate-50">
                    <option value="">-- Or choose a template to load --</option>
                    <option value="mechanic">General Mechanic</option>
                    <option value="panel">Panel Beater</option>
                    <option value="auto_elec">Auto Electrician</option>
                    <option value="generic">Generic Business</option>
                  </select>
                </div>
                <textarea id="tc-textarea" name="terms_conditions" rows="4" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">{{ active_shop.terms_conditions if active_shop else '' }}</textarea>
              </div>
            </div>
          </div>
        </div>
        <div class="px-8 py-5 border-t border-slate-200 bg-slate-50 flex justify-end gap-3 rounded-b-2xl">
          <button type="button" onclick="document.getElementById('manual-setup-modal').classList.add('hidden')" class="rounded-xl border-2 border-slate-300 bg-white px-6 py-3 text-base font-bold text-slate-700 hover:bg-slate-50 transition shadow-sm">Cancel</button>
          <button type="submit" class="rounded-xl bg-indigo-600 px-6 py-3 text-base font-bold text-white hover:bg-indigo-700 shadow-md transition">Save Profile</button>
        </div>
      </form>
    </div>
  </div>'''

# Append before {% endblock %}
new_content = content.replace('{% endblock %}', modal_code + '\n{% endblock %}')

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(new_content)
print("Modal appended")
