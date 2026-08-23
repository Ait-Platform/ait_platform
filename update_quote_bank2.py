import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('.col { display: table-cell; width: 50%;', '.col { display: table-cell; width: 33.33%;')

replacement = '''            <div class="col">
                <h3>Vehicle Details</h3>
                <p><strong>{{ job_card.vehicle.registration_number }}</strong></p>
                <p>Make: {{ job_card.vehicle.make }}</p>
                <p>Model: {{ job_card.vehicle.model or 'N/A' }}</p>
                <p>Year: {{ job_card.vehicle.year or 'N/A' }}</p>
                <p>VIN: {{ job_card.vehicle.vin or 'N/A' }}</p>
                <p>Odometer: {{ job_card.vehicle.mileage ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</p>
            </div>
            <div class="col">
                <h3>Banking Details</h3>
                {% if shop and shop.bank_details %}
                    <p style="font-size: 13px;">{{ shop.bank_details | replace('\n', '<br>') | safe }}</p>
                {% else %}
                    <p style="font-size: 13px;">Please contact us for payment details.</p>
                {% endif %}
                <p style="margin-top: 10px; font-size: 13px;"><strong>Payment Reference:</strong><br>Job Card #{{ job_card.job_number }}</p>
            </div>'''

content = re.sub(
    r"            <div class=\"col\">\s*<h3>Vehicle Details</h3>\s*<p><strong>\{\{ job_card\.vehicle\.registration_number \}\}</strong></p>\s*<p>Make: \{\{ job_card\.vehicle\.make \}\}</p>\s*<p>Model: \{\{ job_card\.vehicle\.model or 'N/A' \}\}</p>\s*<p>Year: \{\{ job_card\.vehicle\.year or 'N/A' \}\}</p>\s*<p>VIN: \{\{ job_card\.vehicle\.vin or 'N/A' \}\}</p>\s*<p>Odometer: \{\{ job_card\.vehicle\.mileage if job_card\.vehicle\.mileage else 'N/A' \}\}</p>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
