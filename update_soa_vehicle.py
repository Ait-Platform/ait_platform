import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

vehicle_block = '''        {% if latest_job_card and latest_job_card.vehicle %}
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 5px; padding: 15px; margin-bottom: 20px; color: #0f172a;">
            <h3 style="margin-top: 0; color: #0f172a; font-size: 14px; text-transform: uppercase; border-bottom: 2px solid #cbd5e1; padding-bottom: 5px;">Vehicle Details</h3>
            <p style="margin-bottom: 0; font-size: 13px;"><strong>{{ latest_job_card.vehicle.registration_number }}</strong> &mdash; 
               Make: {{ latest_job_card.vehicle.make }} | 
               Model: {{ latest_job_card.vehicle.model or 'N/A' }} | 
               Year: {{ latest_job_card.vehicle.year or 'N/A' }} | 
               VIN: {{ latest_job_card.vehicle.vin or 'N/A' }} | 
               Odometer: {{ "{:,.0f}".format(latest_job_card.vehicle.mileage) ~ ' km' if latest_job_card.vehicle.mileage else 'N/A' }}</p>
        </div>
        {% endif %}
        
        <!-- Ledger Table -->'''

content = content.replace("    <!-- Ledger Table -->", vehicle_block)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
