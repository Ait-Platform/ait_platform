import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '<option value="generic">Generic Business</option>',
    '''<option value="generic">Generic Business</option>
                  <option value="protrade">ProTrade Default</option>'''
)

content = content.replace(
    "'generic': '1. Quotes are valid for 7 days.\\n2. Deposit may be required prior to commencement of work.\\n3. Goods remain the property of the business until paid in full.\\n4. Late payments may incur additional fees.'",
    "'generic': '1. Quotes are valid for 7 days.\\n2. Deposit may be required prior to commencement of work.\\n3. Goods remain the property of the business until paid in full.\\n4. Late payments may incur additional fees.',\n      'protrade': '1. THANK YOU FOR YOUR SUPPORT!\\n2. All work is completed to a high standard using genuine parts.\\n3. Parts remain the property of the business until paid in full.\\n4. Vehicles are stored and driven at owner\\'s risk.'"
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
