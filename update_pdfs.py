import re

# --- 1. public_job_card.html ---
with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

page_css = '''
        @page {
            size: A4;
            margin: 1cm 1cm 2cm 1cm;
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

if '@page' not in content:
    content = content.replace('</style>', page_css + '    </style>')

# Remove old E.&O.E. block if it exists
content = re.sub(r'<div style="margin-top: 10px; font-size: 10px; color: #999;">E\.&O\.E\.</div>', '', content)
content = re.sub(r'<div style="margin-top: \d+px; font-size: 10px; color: #[0-9a-fA-F]+;.*?">E\.&O\.E\.</div>', '', content)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

# --- 2. soa_template.html ---
with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

if '@page' not in content2:
    content2 = content2.replace('</style>', page_css + '    </style>')

# Update Next Service Due block to include Odometer
old_next_service = '''        {% if latest_job_card %}
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

content2 = content2.replace(old_next_service, new_next_service)

# Remove old E.&O.E. block
content2 = re.sub(r'<div style="margin-top: 10px; font-size: 10px; color: #9ca3af; font-style: italic; text-align: left;">E\.&O\.E\.</div>', '', content2)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content2)
