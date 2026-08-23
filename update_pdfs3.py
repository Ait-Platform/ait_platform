import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

# 1. Bank Details layout
content2 = content2.replace(
    '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>',
    '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>\n                          <div><strong>Account No:</strong> {{ bank_account.account_number }}</div>'
)

# 2. Odometer in Next Service Due block
# Find the exact block we want to replace
target_block = '''        {% if latest_job_card %}
        <div style="margin-top: 30px; font-size: 14px; font-weight: bold; color: #1e40af; text-align: center; border-top: 2px dashed #bfdbfe; padding-top: 15px;">
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

new_block = '''        {% if latest_job_card %}
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

content2 = content2.replace(target_block, new_block)

# 3. Add @page css
page_css = '''
        @page {
            size: A4;
            margin: 1cm 1cm 1.5cm 1cm;
            @bottom-left {
                content: "E.&O.E.";
                font-size: 10px;
                color: #9ca3af;
                font-style: italic;
            }
            @bottom-center {
                content: "Page " counter(page) " of " counter(pages);
                font-size: 10px;
                color: #9ca3af;
            }
            @bottom-right {
                content: "Powered by AIT";
                font-size: 10px;
                color: #9ca3af;
                font-style: italic;
            }
        }
'''

if '@page' not in content2:
    content2 = content2.replace('</style>', page_css + '    </style>')

# 4. Remove old E.&O.E. block
content2 = re.sub(r'<div style="margin-top: 10px; font-size: 10px; color: #9ca3af; font-style: italic; text-align: left;">E\.&O\.E\.</div>', '', content2)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content2)
