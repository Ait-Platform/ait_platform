import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_details = '''            <p class="text-slate-600 text-sm"><span class="font-semibold">Make:</span> {{ job_card.vehicle.make }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Model:</span> {{ job_card.vehicle.model or 'Unknown' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
          </div>'''

new_details = '''            <p class="text-slate-600 text-sm"><span class="font-semibold">Make:</span> {{ job_card.vehicle.make }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Model:</span> {{ job_card.vehicle.model or 'Unknown' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Mileage:</span> {{ job_card.mileage or 'N/A' }}</p>
          </div>'''

content = content.replace(old_details, new_details)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
