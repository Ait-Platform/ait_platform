import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Odometer:</span> {{ job_card.vehicle.mileage if job_card.vehicle.mileage else 'N/A' }}</p>
          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Next Service Due:</span> {{ job_card.next_service_due if job_card.next_service_due else 'N/A' }}</p>'''

content = content.replace(
    '''          <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Odometer:</span> {{ job_card.vehicle.mileage if job_card.vehicle.mileage else 'N/A' }}</p>''',
    replacement
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
