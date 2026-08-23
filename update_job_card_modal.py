import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Make the modal content scrollable if it exceeds screen height
content = content.replace(
    'class="bg-white rounded-2xl w-full max-w-md shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out]"',
    'class="bg-white rounded-2xl w-full max-w-lg shadow-2xl overflow-hidden animate-[fadeIn_0.2s_ease-out] flex flex-col max-h-[90vh]"'
)

content = content.replace(
    '<div class="p-6">',
    '<div class="p-6 overflow-y-auto">'
)

# Convert space-y-4 to grid
replacement = '''          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Make</label>
              <input type="text" name="make" value="{{ job_card.vehicle.make or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Model</label>
              <input type="text" name="model" value="{{ job_card.vehicle.model or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Year</label>
              <input type="number" name="year" value="{{ job_card.vehicle.year or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div>
              <label class="block text-sm font-bold text-slate-700 mb-1">Odometer (km)</label>
              <input type="number" id="modal_mileage" name="mileage" value="{{ job_card.vehicle.mileage or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div class="col-span-2">
              <label class="block text-sm font-bold text-slate-700 mb-1">VIN Number</label>
              <input type="text" name="vin" value="{{ job_card.vehicle.vin or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
            <div class="col-span-2">
              <label class="block text-sm font-bold text-slate-700 mb-1">Next Service Due</label>
              <input type="text" id="modal_next_service" name="next_service_due" value="{{ job_card.next_service_due or '' }}" class="w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none">
            </div>
          </div>'''

content = re.sub(
    r"          <div class=\"space-y-4\">\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Make</label>\s*<input type=\"text\" name=\"make\" value=\"\{\{ job_card\.vehicle\.make or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Model</label>\s*<input type=\"text\" name=\"model\" value=\"\{\{ job_card\.vehicle\.model or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Year</label>\s*<input type=\"number\" name=\"year\" value=\"\{\{ job_card\.vehicle\.year or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">VIN Number</label>\s*<input type=\"text\" name=\"vin\" value=\"\{\{ job_card\.vehicle\.vin or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Odometer \(km\)</label>\s*<input type=\"number\" id=\"modal_mileage\" name=\"mileage\" value=\"\{\{ job_card\.vehicle\.mileage or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*<div>\s*<label class=\"block text-sm font-bold text-slate-700 mb-1\">Next Service Due</label>\s*<input type=\"text\" id=\"modal_next_service\" name=\"next_service_due\" value=\"\{\{ job_card\.next_service_due or '' \}\}\" class=\"w-full rounded-lg border border-slate-300 px-3 py-2 text-slate-900 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none\">\s*</div>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
