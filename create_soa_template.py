import re

with open('templates/program_billing/metsoa.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace header references
text = text.replace('Statement for Utilities', 'Statement of Account (SOA)')
text = text.replace('sendMetsoaEmail()', 'sendSoaEmail()')

# Change customer details from blank to Tenant specific
old_customer = '''          <div class="flex"><span class="w-24 font-semibold">Customer</span><span></span></div>
          <div class="flex"><span class="w-24 font-semibold">Email</span><span></span></div>'''
new_customer = '''          <div class="flex"><span class="w-24 font-semibold">Tenant Name</span><span>{{ tenant.name }}</span></div>
          <div class="flex"><span class="w-24 font-semibold">Email</span><span>{{ tenant.email or '' }}</span></div>'''
text = text.replace(old_customer, new_customer)

old_address = '''<div class="flex"><span class="w-24 font-semibold">Address</span><span></span></div>'''
new_address = '''<div class="flex"><span class="w-24 font-semibold">Address</span><span>{{ tenant.address or property.name }}</span></div>'''
text = text.replace(old_address, new_address)

# Inject mapped charges table right before Grand Total
grand_total_section_start = text.find('<!-- Grand Total -->')

if grand_total_section_start == -1:
    print('Could not find grand total section!')
else:
    mapped_charges_html = '''
      <!-- Mapped Charges -->
      {% if mapped_charges %}
      <div class="mb-6 page-break-inside-avoid">
        <div class="flex justify-between items-end border-b border-gray-400 pb-2 mb-3">
          <h2 class="text-lg font-bold">Municipal/Fixed Charges</h2>
        </div>
        <table class="w-full text-sm mb-2">
          <thead>
            <tr class="border-b border-gray-300">
              <th class="text-left py-1 w-3/4">Description</th>
              <th class="text-right py-1 w-1/4">Amount (R)</th>
            </tr>
          </thead>
          <tbody>
            {% for charge in mapped_charges %}
            <tr>
              <td class="py-1">{{ charge.description }}</td>
              <td class="text-right py-1">{{ "%.2f"|format(charge.amount) }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        <div class="flex justify-end">
          <div class="w-1/4 border-t border-gray-400 pt-1 flex justify-between font-bold">
            <span>Subtotal</span>
            <span>R {{ "%.2f"|format(mapped_total) }}</span>
          </div>
        </div>
      </div>
      {% endif %}
'''
    text = text[:grand_total_section_start] + mapped_charges_html + text[grand_total_section_start:]

# Change script fetch URL
text = text.replace("{{ url_for('billing_bp.email_metsoa', property_id=property.id, month=month) }}", 
                    "{{ url_for('billing_bp.email_soa', tenant_id=tenant.id, month=month) }}")

with open('templates/program_billing/soa_document.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Created soa_document.html')
