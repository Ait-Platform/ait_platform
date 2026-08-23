import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Change grid-cols-1 md:grid-cols-2 to grid-cols-1 md:grid-cols-3
content = content.replace(
    '<div class="grid grid-cols-1 md:grid-cols-2 gap-6">',
    '<div class="grid grid-cols-1 md:grid-cols-3 gap-6">'
)

replacement = '''          <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">
            <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
              <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Vehicle Details</h3>
              <button type="button" onclick="document.getElementById('edit-vehicle-modal').classList.remove('hidden')" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">
                <i class="fas fa-edit mr-1"></i>Edit
              </button>
            </div>
            <p class="font-bold text-slate-900 text-lg mb-1">{{ job_card.vehicle.registration_number }}</p>
            <p class="text-slate-600 text-sm"><span class="font-semibold">Make:</span> {{ job_card.vehicle.make }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Model:</span> {{ job_card.vehicle.model or 'Unknown' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Odometer:</span> {{ "{:,.0f}".format(job_card.vehicle.mileage) ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</p>
          </div>
          
          <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">
            <div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">
              <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Banking Details</h3>
              <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">
                <i class="fas fa-edit mr-1"></i>Edit
              </a>
            </div>
            {% if bank_account %}
                {% if bank_account.raw_details %}
                    <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ bank_account.raw_details }}</p>
                {% else %}
                    <p class="text-slate-600 text-sm"><span class="font-semibold">Bank:</span> {{ bank_account.bank_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account:</span> {{ bank_account.account_name }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account No:</span> {{ bank_account.account_number }}</p>
                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}</p>
                {% endif %}
            {% elif shop and shop.bank_details %}
                <p class="text-slate-600 text-sm whitespace-pre-wrap">{{ shop.bank_details }}</p>
            {% else %}
                <p class="text-slate-500 text-sm italic">No bank details configured. <a href="{{ url_for('mechanic_bp.bank_accounts') }}" class="text-indigo-600 hover:underline">Add one</a>.</p>
            {% endif %}
          </div>'''

content = re.sub(
    r"          <div class=\"bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group\">\s*<div class=\"flex justify-between items-center mb-4 border-b border-slate-100 pb-2\">\s*<h3 class=\"text-sm font-bold text-slate-500 uppercase tracking-wider\">Vehicle Details</h3>.*?<p class=\"text-slate-600 text-sm mt-1\"><span class=\"font-semibold\">Odometer:</span> \{\{ \"\{:,\.0f\}\"\.format\(job_card\.vehicle\.mileage\) ~ ' km' if job_card\.vehicle\.mileage else 'N/A' \}\}</p>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
