import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Replace the Vehicle Details header
replacement = '''        <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">
          <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
            <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Vehicle Details</h3>
            <button type="button" onclick="document.getElementById('edit-vehicle-modal').classList.remove('hidden')" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">
              <i class="fas fa-edit mr-1"></i>Edit
            </button>
          </div>'''

content = re.sub(
    r"        <div class=\"bg-white border border-slate-200 rounded-xl p-5 shadow-sm\">\s*<h3 class=\"text-sm font-bold text-slate-500 uppercase tracking-wider mb-4 border-b border-slate-100 pb-2\">Vehicle Details</h3>",
    replacement,
    content,
    flags=re.DOTALL
)

# 2. Append the modal AT THE VERY END OF THE FILE, but BEFORE the final {% endblock %}
# We'll use an rsplit to split from the right by {% endblock %} exactly once.

modal_html = '''
<!-- Edit Vehicle Modal -->
  <div id="edit-vehicle-modal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
    <div class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]">
      <div class="p-6">
        <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
          <h2 class="text-xl font-bold text-slate-800">Edit Vehicle Details</h2>
          <button type="button" onclick="document.getElementById('edit-vehicle-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 transition">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
          </button>
        </div>
        <form method="POST" action="{{ url_for('mechanic_bp.edit_job_vehicle', id=job_card.id) }}">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Make</label>
              <input type="text" name="make" value="{{ job_card.vehicle.make or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Model</label>
              <input type="text" name="model" value="{{ job_card.vehicle.model or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">VIN Number</label>
              <input type="text" name="vin" value="{{ job_card.vehicle.vin or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Odometer (km)</label>
              <input type="number" id="modal_mileage" name="mileage" value="{{ job_card.vehicle.mileage or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Next Service Due</label>
              <input type="text" id="modal_next_service" name="next_service_due" value="{{ job_card.next_service_due or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
          </div>
          
          <div class="flex justify-end gap-3 mt-8 border-t border-slate-100 pt-4">
            <button type="button" onclick="document.getElementById('edit-vehicle-modal').classList.add('hidden')" class="px-5 py-2.5 rounded-lg border border-slate-300 text-slate-700 font-bold hover:bg-slate-50 transition">Cancel</button>
            <button type="submit" class="px-5 py-2.5 rounded-lg bg-indigo-600 text-white font-bold hover:bg-indigo-700 shadow-sm transition">Save Changes</button>
          </div>
        </form>
      </div>
    </div>
  </div>
  <script>
    document.getElementById('modal_mileage').addEventListener('input', function(e) {
        const val = parseInt(e.target.value);
        const nextInput = document.getElementById('modal_next_service');
        if (!isNaN(val) && val > 0) {
            nextInput.value = (val + 10000) + " km";
        } else {
            nextInput.value = "";
        }
    });
  </script>
{% endblock %}
'''

# Splitting by {% endblock %} from the right side exactly once to guarantee we replace the final endblock!
parts = content.rsplit('{% endblock %}', 1)
if len(parts) == 2:
    content = parts[0] + modal_html + parts[1]

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
