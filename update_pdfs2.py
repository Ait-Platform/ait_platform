import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

new_next_service = '''        {% if latest_job_card %}
        <div style="margin-top: 30px; font-size: 14px; font-weight: bold; color: #1e40af; text-align: center; border-top: 2px dashed #bfdbfe; padding-top: 15px;">
            {% if latest_job_card.vehicle and latest_job_card.vehicle.mileage %}
                ODOMETER: {{ "{:,.0f}".format(latest_job_card.vehicle.mileage) }} km &nbsp; | &nbsp; 
            {% endif %}
            NEXT SERVICE DUE: 
            {% if latest_job_card.next_service_due and latest_job_card.next_service_due|lower != 'n/a' %}
                {{ latest_job_card.next_service_due }}
            {% elif latest_job_card.vehicle and latest_job_card.vehicle.mileage %}
                {{ "{:,.0f}".format(latest_job_card.vehicle.mileage + 10000) }} km
            {% else %}
                To Be Determined
            {% endif %}
        </div>
        {% endif %}'''

content2 = re.sub(
    r'\{% if latest_job_card %\}.*?NEXT SERVICE DUE:.*?\{% endif %\}',
    new_next_service,
    content2,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content2)
