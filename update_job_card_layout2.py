import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Change grid from 3 to 2 columns
content = content.replace(
    '<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">',
    '<div class="grid grid-cols-1 md:grid-cols-2 gap-6">'
)

vehicle_regex = r'<div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group">\s*<div class="flex justify-between items-center mb-4 border-b border-slate-100 pb-2">\s*<h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Vehicle Details</h3>.*?Next Service Due:</span>.*?</div>'

vehicle_match = re.search(vehicle_regex, content, re.DOTALL)
if vehicle_match:
    vehicle_block = vehicle_match.group(0)
    
    # Remove Vehicle Block from its current position
    content = content.replace(vehicle_block, '')
    
    # We want to replace it with a wide horizontal block, placed after the grid closes.
    new_vehicle_block = '''
        <!-- Vehicle Details Horizontal -->
        <div class="bg-white border border-slate-200 rounded-xl p-5 shadow-sm relative group mt-6">
            <div class="flex justify-between items-center mb-2 border-b border-slate-100 pb-2">
              <h3 class="text-sm font-bold text-slate-500 uppercase tracking-wider">Vehicle Details</h3>
              <button type="button" onclick="document.getElementById('edit-vehicle-modal').classList.remove('hidden')" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition px-2 py-1 bg-indigo-50 rounded hidden group-hover:block border border-indigo-200">
                <i class="fas fa-edit mr-1"></i>Edit
              </button>
            </div>
            <p class="text-slate-800 text-sm">
                <strong class="text-slate-900 text-base">{{ job_card.vehicle.registration_number }}</strong> &mdash; 
                <span class="font-semibold text-slate-600 ml-2">Make:</span> {{ job_card.vehicle.make }}
                <span class="font-semibold text-slate-600 ml-3">Model:</span> {{ job_card.vehicle.model or 'N/A' }}
                <span class="font-semibold text-slate-600 ml-3">Year:</span> {{ job_card.vehicle.year or 'N/A' }}
                <span class="font-semibold text-slate-600 ml-3">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}
                <span class="font-semibold text-slate-600 ml-3">Odometer:</span> {{ "{:,.0f}".format(job_card.vehicle.mileage) ~ ' km' if job_card.vehicle.mileage else 'N/A' }}
                <span class="font-semibold text-slate-600 ml-3">Next Service Due:</span> {{ job_card.next_service_due if job_card.next_service_due else 'N/A' }}
            </p>
        </div>'''
    
    # Find the end of the grid: it's the </div> before <!-- Parts & Labor Lines -->
    parts_labor_index = content.find('<!-- Parts & Labor Lines -->')
    if parts_labor_index != -1:
        # Insert before Parts & Labor Lines
        content = content[:parts_labor_index] + new_vehicle_block + '\n\n        ' + content[parts_labor_index:]
        
    with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
        f.write(content)
