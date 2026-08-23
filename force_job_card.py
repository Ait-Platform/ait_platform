import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''              <div class="text-sm">
                <span class="text-slate-500 block mb-1">Make / Model</span>
                <span class="font-bold text-slate-800">{{ job_card.vehicle.make }} {{ job_card.vehicle.model }} ({{ job_card.vehicle.year }})</span>
              </div>
              <div class="text-sm">
                <span class="text-slate-500 block mb-1">Registration</span>
                <span class="font-bold text-slate-800 bg-slate-100 px-2 py-1 rounded inline-block">{{ job_card.vehicle.registration_number }}</span>
              </div>
              <div class="text-sm">
                <span class="text-slate-500 block mb-1">VIN / Chassis</span>
                <span class="font-bold text-slate-800 font-mono">{{ job_card.vehicle.vin or 'N/A' }}</span>
              </div>
              <div class="text-sm">
                <span class="text-slate-500 block mb-1">Odometer / Mileage</span>
                <span class="font-bold text-slate-800">{{ job_card.vehicle.mileage if job_card.vehicle.mileage else 'N/A' }}</span>
              </div>
              <div class="text-sm">
                <span class="text-slate-500 block mb-1">Next Service Due</span>
                <span class="font-bold text-slate-800">{{ job_card.next_service_due if job_card.next_service_due else 'N/A' }}</span>
              </div>'''

# Let's find the exact block using a more generic regex
content = re.sub(
    r"<div class=\"text-sm\">\s*<span class=\"text-slate-500 block mb-1\">Make / Model</span>.*?Odometer / Mileage</span>\s*<span class=\"font-bold text-slate-800\">\{\{ job_card\.vehicle\.mileage if job_card\.vehicle\.mileage else 'N/A' \}\}</span>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
