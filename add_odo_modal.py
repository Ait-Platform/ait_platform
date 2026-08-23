import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Make "To Be Determined" a clickable button that opens the dedicated Odometer modal
# First, let's find where we display Next Service Due
regex = r'(<span class="text-slate-600 ml-3">Next Service Due:</span> <span class="font-bold text-slate-900">\{\{ job_card\.next_service_due if job_card\.next_service_due else \'N/A\' \}\}</span>)'
replacement = '''<span class="text-slate-600 ml-3">Next Service Due:</span> 
                {% if job_card.next_service_due and job_card.next_service_due|lower != 'n/a' %}
                    <span class="font-bold text-slate-900">{{ job_card.next_service_due }}</span>
                {% elif job_card.vehicle and job_card.vehicle.mileage %}
                    <span class="font-bold text-slate-900">{{ "{:,.0f}".format(job_card.vehicle.mileage + 15000) }} km</span>
                {% else %}
                    <button onclick="document.getElementById('quick-odo-modal').classList.remove('hidden')" class="font-bold text-rose-600 hover:text-rose-800 bg-rose-50 px-2 py-0.5 rounded border border-rose-200 transition">
                        <i class="fas fa-exclamation-circle mr-1"></i> To Be Determined (Add Odometer)
                    </button>
                {% endif %}'''

content = re.sub(regex, replacement, content)


# Add the modal html at the bottom of the page before the scripts
modal_html = '''
  <!-- Quick Odometer Modal -->
  <div id="quick-odo-modal" class="fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl shadow-xl w-full max-w-md overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
        <h3 class="font-bold text-slate-800 text-lg"><i class="fas fa-tachometer-alt text-indigo-600 mr-2"></i> Enter Odometer Reading</h3>
        <button onclick="document.getElementById('quick-odo-modal').classList.add('hidden')" class="text-slate-400 hover:text-rose-500 transition">
          <i class="fas fa-times text-xl"></i>
        </button>
      </div>
      <div class="p-6">
        <p class="text-sm text-slate-500 mb-6">Physical paperwork often contains the exact odometer reading. Enter it below to automatically calculate the Next Service Due date.</p>
        <form method="POST" action="{{ url_for('mechanic_bp.quick_update_odometer', id=job_card.id) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
          
          <div class="mb-6">
            <label class="block text-sm font-bold text-slate-700 mb-2">Current Odometer (km)</label>
            <div class="relative">
              <input type="number" name="mileage" required placeholder="e.g. 150000" class="w-full rounded-lg border-2 border-slate-300 px-4 py-3 text-lg font-bold text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none pr-12">
              <span class="absolute right-4 top-3.5 font-bold text-slate-400">km</span>
            </div>
          </div>
          
          <button type="submit" class="w-full py-3 rounded-lg bg-indigo-600 text-white font-bold text-lg hover:bg-indigo-700 shadow-sm transition">
            Save & Calculate Service
          </button>
        </form>
      </div>
    </div>
  </div>
'''

content = content.replace("  {% endblock %}", modal_html + "\n  {% endblock %}")

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
