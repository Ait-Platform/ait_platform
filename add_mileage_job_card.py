import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_vin = '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
        </div>'''

new_vin = '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Mileage:</span> {{ job_card.vehicle.mileage ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</p>
        </div>'''

content = content.replace(old_vin, new_vin)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
