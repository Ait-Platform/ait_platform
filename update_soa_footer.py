import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''      {% if latest_job_card %}
      <div style="margin-top: 30px; padding: 15px; background-color: #f8fafc; border: 1px dashed #cbd5e1; font-size: 14px; font-weight: bold; text-align: center; color: #334155; border-radius: 6px;">
          NEXT SERVICE DUE: 
          {% if latest_job_card.next_service_due and latest_job_card.next_service_due|lower != 'n/a' %}
              {{ latest_job_card.next_service_due }}
          {% elif latest_job_card.vehicle and latest_job_card.vehicle.mileage %}
              {{ "{:,.0f}".format(latest_job_card.vehicle.mileage + 10000) }} km
          {% else %}
              To Be Determined
          {% endif %}
      </div>
      {% endif %}

      <div style="margin-top: 20px; font-size: 12px; color: #4b5563; text-align: center; font-weight: bold;">
          Thank you for your business! Only genuine parts used. Professional services guaranteed.
      </div>

      <div style="margin-top: 10px; font-size: 10px; color: #9ca3af; font-style: italic; text-align: left;">E.&O.E.</div>'''

content = re.sub(
    r"      <div style=\"margin-top: 30px; font-size: 10px; color: #9ca3af; font-style: italic;\">E\.&O\.E\.</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
