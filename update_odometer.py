import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('Mileage (km)', 'Odometer / Mileage')
content = content.replace('placeholder="e.g. 150000"', 'placeholder="e.g. 150000"')

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace(
    '''<p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Mileage:</span> {{ job_card.vehicle.mileage ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</p>''',
    '''<p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Odometer:</span> {{ job_card.vehicle.mileage if job_card.vehicle.mileage else 'N/A' }}</p>'''
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content2)
