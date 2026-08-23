import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'(<h3 style="color: #0f172a; border-bottom: 2px solid #cbd5e1;">Banking Details</h3>.*?)(<p style="margin-top: 10px; font-size: 13px;">Payment Reference:<br><strong>\{\{\s*job_card\.job_number\.split\(\'-\'\)\[-1\]\s*\}\}</strong></p>)'

dynamic_html = '''<h3 style="color: #0f172a; border-bottom: 2px solid #cbd5e1;">Payment Instructions</h3>
                
                {% if job_card.payment_method == 'eWallet' %}
                    <p style="font-size: 13px; font-weight: bold; color: #4f46e5; margin-bottom: 5px;">eWallet / Send Cash</p>
                    <p style="font-size: 13px; line-height: 1.5;">Please send the payment via eWallet or Cash Send to our business phone number:<br>
                    <strong>{{ shop.phone if shop and shop.phone else 'Please contact us for the number.' }}</strong></p>
                
                {% elif job_card.payment_method == 'Cash' %}
                    <p style="font-size: 13px; font-weight: bold; color: #059669; margin-bottom: 5px;">Physical Cash</p>
                    <p style="font-size: 13px; line-height: 1.5;">Please pay the deposit in Cash at the workshop so we can commence work.</p>
                
                {% else %}
                    {% if bank_account %}
                        {% if bank_account.raw_details %}
                            <p style="font-size: 13px; white-space: pre-wrap;">{{ bank_account.raw_details }}</p>
                        {% else %}
                            <div style="font-size: 13px;">
                                <div>Bank: <strong>{{ bank_account.bank_name }}</strong></div>
                                <div>Account Name: <strong>{{ bank_account.account_name }}</strong></div>
                                <div>BSB: <strong>{{ bank_account.bsb_branch }}</strong></div>
                                <div>Account No: <strong>{{ bank_account.account_number }}</strong></div>
                                {% if bank_account.swift_code %}<div>SWIFT: <strong>{{ bank_account.swift_code }}</strong></div>{% endif %}
                            </div>
                        {% endif %}
                    {% elif shop and shop.bank_details %}
                        <p style="font-size: 13px;">{{ shop.bank_details | replace('\\n', '<br>') | safe }}</p>
                    {% else %}
                        <p style="font-size: 13px;">Please contact us for payment details.</p>
                    {% endif %}
                {% endif %}
                
                '''

content = re.sub(regex, dynamic_html + r'\2', content, flags=re.DOTALL)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
