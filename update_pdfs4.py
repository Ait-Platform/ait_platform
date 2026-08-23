import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = re.sub(
    r'NEXT SERVICE DUE:\s*\{% if latest_job_card\.next_service_due and latest_job_card\.next_service_due\|lower != \'n/a\' %\}',
    r'{% if latest_job_card.vehicle and latest_job_card.vehicle.mileage %}ODOMETER: {{ "{:,.0f}".format(latest_job_card.vehicle.mileage) }} km &nbsp; | &nbsp; {% endif %}NEXT SERVICE DUE:\n            {% if latest_job_card.next_service_due and latest_job_card.next_service_due|lower != \'n/a\' %}',
    content2,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content2)
