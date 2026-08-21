import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_pdf = '''                  <p><strong>Make:</strong> {{ job_card.vehicle.make }}</p>
                  <p><strong>Model:</strong> {{ job_card.vehicle.model or 'Unknown' }}</p>
                  <p><strong>Year:</strong> {{ job_card.vehicle.year or 'N/A' }}</p>
                  <p><strong>VIN:</strong> {{ job_card.vehicle.vin or 'N/A' }}</p>
              </div>'''

new_pdf = '''                  <p><strong>Make:</strong> {{ job_card.vehicle.make }}</p>
                  <p><strong>Model:</strong> {{ job_card.vehicle.model or 'Unknown' }}</p>
                  <p><strong>Year:</strong> {{ job_card.vehicle.year or 'N/A' }}</p>
                  <p><strong>VIN:</strong> {{ job_card.vehicle.vin or 'N/A' }}</p>
                  {% if job_card.mileage %}
                  <p><strong>Mileage:</strong> {{ job_card.mileage }}</p>
                  {% endif %}
              </div>'''

content = content.replace(old_pdf, new_pdf)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
