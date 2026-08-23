import re

# 1. public_job_card.html
with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("Make: {{ job_card.vehicle.make }} | ", "Make: <strong>{{ job_card.vehicle.make }}</strong> | ")
content = content.replace("Model: {{ job_card.vehicle.model or 'N/A' }} | ", "Model: <strong>{{ job_card.vehicle.model or 'N/A' }}</strong> | ")
content = content.replace("Year: {{ job_card.vehicle.year or 'N/A' }} | ", "Year: <strong>{{ job_card.vehicle.year or 'N/A' }}</strong> | ")
content = content.replace("VIN: {{ job_card.vehicle.vin or 'N/A' }} | ", "VIN: <strong>{{ job_card.vehicle.vin or 'N/A' }}</strong> | ")
content = content.replace("Odometer: {{ \"{:,.0f}\".format(job_card.vehicle.mileage) ~ ' km' if job_card.vehicle.mileage else 'N/A' }}", "Odometer: <strong>{{ \"{:,.0f}\".format(job_card.vehicle.mileage) ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</strong>")

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

# 2. soa_template.html
with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("Make: {{ latest_job_card.vehicle.make }} | ", "Make: <strong>{{ latest_job_card.vehicle.make }}</strong> | ")
content = content.replace("Model: {{ latest_job_card.vehicle.model or 'N/A' }} | ", "Model: <strong>{{ latest_job_card.vehicle.model or 'N/A' }}</strong> | ")
content = content.replace("Year: {{ latest_job_card.vehicle.year or 'N/A' }} | ", "Year: <strong>{{ latest_job_card.vehicle.year or 'N/A' }}</strong> | ")
content = content.replace("VIN: {{ latest_job_card.vehicle.vin or 'N/A' }} | ", "VIN: <strong>{{ latest_job_card.vehicle.vin or 'N/A' }}</strong> | ")
content = content.replace("Odometer: {{ \"{:,.0f}\".format(latest_job_card.vehicle.mileage) ~ ' km' if latest_job_card.vehicle.mileage else 'N/A' }}", "Odometer: <strong>{{ \"{:,.0f}\".format(latest_job_card.vehicle.mileage) ~ ' km' if latest_job_card.vehicle.mileage else 'N/A' }}</strong>")

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)

# 3. job_card.html
with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<span class="font-semibold text-slate-600 ml-2">Make:</span> {{ job_card.vehicle.make }}', '<span class="text-slate-600 ml-2">Make:</span> <span class="font-bold text-slate-900">{{ job_card.vehicle.make }}</span>')
content = content.replace('<span class="font-semibold text-slate-600 ml-3">Model:</span> {{ job_card.vehicle.model or \'N/A\' }}', '<span class="text-slate-600 ml-3">Model:</span> <span class="font-bold text-slate-900">{{ job_card.vehicle.model or \'N/A\' }}</span>')
content = content.replace('<span class="font-semibold text-slate-600 ml-3">Year:</span> {{ job_card.vehicle.year or \'N/A\' }}', '<span class="text-slate-600 ml-3">Year:</span> <span class="font-bold text-slate-900">{{ job_card.vehicle.year or \'N/A\' }}</span>')
content = content.replace('<span class="font-semibold text-slate-600 ml-3">VIN:</span> {{ job_card.vehicle.vin or \'N/A\' }}', '<span class="text-slate-600 ml-3">VIN:</span> <span class="font-bold text-slate-900">{{ job_card.vehicle.vin or \'N/A\' }}</span>')
content = content.replace('<span class="font-semibold text-slate-600 ml-3">Odometer:</span> {{ "{:,.0f}".format(job_card.vehicle.mileage) ~ \' km\' if job_card.vehicle.mileage else \'N/A\' }}', '<span class="text-slate-600 ml-3">Odometer:</span> <span class="font-bold text-slate-900">{{ "{:,.0f}".format(job_card.vehicle.mileage) ~ \' km\' if job_card.vehicle.mileage else \'N/A\' }}</span>')
content = content.replace('<span class="font-semibold text-slate-600 ml-3">Next Service Due:</span> {{ job_card.next_service_due if job_card.next_service_due else \'N/A\' }}', '<span class="text-slate-600 ml-3">Next Service Due:</span> <span class="font-bold text-slate-900">{{ job_card.next_service_due if job_card.next_service_due else \'N/A\' }}</span>')

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

