import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# CSS Fixes
content = content.replace(
    ".col { display: table-cell; width: 33.33%; padding: 10px; vertical-align: top; border: 1px solid #f0f0f0; background: #fdfdfd; }",
    ".col { display: table-cell; width: 50%; padding: 15px; vertical-align: top; border: 1px solid #e2e8f0; background: #f8fafc; color: #0f172a; }"
)

content = content.replace(
    "body { font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #333; line-height: 1.5; margin: 0; padding: 20px; background: #fff; }",
    "body { font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #0f172a; line-height: 1.5; margin: 0; padding: 20px; background: #fff; font-weight: 500; }"
)

# HTML layout rewrite
old_grid = r'<div class="details-grid">.*?</div>\s*</div>\s*<table>'
new_grid = '''<div class="details-grid">
            <div class="col">
                <h3 style="color: #0f172a; border-bottom: 2px solid #cbd5e1;">Client Details</h3>
                <p><strong>{{ job_card.vehicle.client.name }}</strong></p>
                <p>Phone: {{ job_card.vehicle.client.phone or 'N/A' }}</p>
                <p>Email: {{ job_card.vehicle.client.email or 'N/A' }}</p>
            </div>
            <div class="col">
                <h3 style="color: #0f172a; border-bottom: 2px solid #cbd5e1;">Banking Details</h3>
                {% if bank_account %}
                    {% if bank_account.raw_details %}
                        <p style="font-size: 13px; white-space: pre-wrap;">{{ bank_account.raw_details }}</p>
                    {% else %}
                        <div style="font-size: 13px;">
                            <div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>
                            <div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>
                            <div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>
                            {% if bank_account.swift_code %}<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>{% endif %}
                        </div>
                    {% endif %}
                {% elif shop and shop.bank_details %}
                    <p style="font-size: 13px;">{{ shop.bank_details | replace('\\n', '<br>') | safe }}</p>
                {% else %}
                    <p style="font-size: 13px;">Please contact us for payment details.</p>
                {% endif %}
                <p style="margin-top: 10px; font-size: 13px;"><strong>Payment Reference:</strong><br>Job Card #{{ job_card.job_number }}</p>
            </div>
        </div>
        
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 5px; padding: 15px; margin-bottom: 30px; color: #0f172a;">
            <h3 style="margin-top: 0; color: #0f172a; font-size: 14px; text-transform: uppercase; border-bottom: 2px solid #cbd5e1; padding-bottom: 5px;">Vehicle Details</h3>
            <p style="margin-bottom: 0;"><strong>{{ job_card.vehicle.registration_number }}</strong> &mdash; 
               Make: {{ job_card.vehicle.make }} | 
               Model: {{ job_card.vehicle.model or 'N/A' }} | 
               Year: {{ job_card.vehicle.year or 'N/A' }} | 
               VIN: {{ job_card.vehicle.vin or 'N/A' }} | 
               Odometer: {{ "{:,.0f}".format(job_card.vehicle.mileage) ~ ' km' if job_card.vehicle.mileage else 'N/A' }}</p>
        </div>

        <table>'''

content = re.sub(old_grid, new_grid, content, flags=re.DOTALL)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
