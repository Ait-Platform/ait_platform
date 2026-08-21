with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

card_original = '''            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
          </div>'''

card_new = '''            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Year:</span> {{ job_card.vehicle.year or 'N/A' }}</p>
            <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">VIN:</span> {{ job_card.vehicle.vin or 'N/A' }}</p>
            <div class="grid grid-cols-2 gap-2 mt-2 pt-2 border-t border-slate-100">
              <p class="text-slate-600 text-xs"><span class="font-semibold">Engine No:</span><br>{{ job_card.vehicle.engine_no or '-' }}</p>
              <p class="text-slate-600 text-xs"><span class="font-semibold">License No:</span><br>{{ job_card.vehicle.disk_license_no or '-' }}</p>
              <p class="text-slate-600 text-xs"><span class="font-semibold">GVM:</span> {{ job_card.vehicle.gvm or '-' }} kg</p>
              <p class="text-slate-600 text-xs"><span class="font-semibold">Tare:</span> {{ job_card.vehicle.tare or '-' }} kg</p>
            </div>
          </div>'''

content = content.replace(card_original, card_new)
with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_mechanic/invoice_view.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace(card_original, card_new)
with open('templates/program_mechanic/invoice_view.html', 'w', encoding='utf-8') as f:
    f.write(content2)

print("Updated job_card.html and invoice_view.html")
