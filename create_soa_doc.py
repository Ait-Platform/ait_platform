import re

with open('templates/program_billing/metsoa.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace header references
text = text.replace('Exception Statement for Utilities', 'Statement of Account (SOA)')
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

# Subtotal Utilities
text = text.replace('DUE TO ETHEKWINI', 'SUBTOTAL (UTILITIES)')

# Mapped charges insertion before the final style block
# Actually, let's replace the grand_total display logic
# The original metsoa.html calculates grand_total = electricity.subtotal + water.total
# Let's replace the Due row with a modified block

old_total_row = '''          <tr class="border-t-2 border-gray-600 bg-gray-50 font-bold">
            <td colspan="5" class="py-2 px-2 text-lg">SUBTOTAL (UTILITIES)</td>
            <td class="py-2 px-1"></td>
            <td colspan="3" class="py-2 px-1"></td>
            <td class="py-2 px-2 text-right text-lg">R{{ "%.2f"|format(grand_total) }}</td>
          </tr>
        </tbody>
      </table>'''

old_total_row_alt = '''          <tr class="border-t-2 border-gray-600 bg-gray-50 font-bold">
            <td colspan="5" class="py-2 px-2 text-lg">DUE TO ETHEKWINI</td>
            <td class="py-2 px-1"></td>
            <td colspan="3" class="py-2 px-1"></td>
            <td class="py-2 px-2 text-right text-lg">R{{ "%.2f"|format(grand_total) }}</td>
          </tr>
        </tbody>
      </table>'''

new_total_row = '''          <tr class="border-t-2 border-gray-600 bg-gray-50 font-bold">
            <td colspan="5" class="py-2 px-2 text-lg">SUBTOTAL (UTILITIES)</td>
            <td class="py-2 px-1"></td>
            <td colspan="3" class="py-2 px-1"></td>
            <td class="py-2 px-2 text-right text-lg">R{{ "%.2f"|format(electricity.subtotal + water.total) }}</td>
          </tr>
          
          {% if mapped_charges %}
            <tr class="bg-white"><td colspan="10" class="py-4"></td></tr>
            <tr class="border-t-2 border-gray-600 bg-gray-100 font-bold">
              <td colspan="5" class="py-2 px-2 text-lg text-purple-800">MUNICIPAL & FIXED CHARGES</td>
              <td colspan="5"></td>
            </tr>
            {% for charge in mapped_charges %}
            <tr>
              <td colspan="5" class="py-2 px-2 font-semibold">{{ charge.description }}</td>
              <td colspan="4"></td>
              <td class="py-2 px-2 text-right">R{{ "%.2f"|format(charge.amount) }}</td>
            </tr>
            {% endfor %}
            <tr class="border-t border-gray-300 font-bold bg-gray-50">
              <td colspan="5" class="py-2 px-2 text-md">SUBTOTAL (FIXED)</td>
              <td colspan="4"></td>
              <td class="py-2 px-2 text-right">R{{ "%.2f"|format(mapped_total) }}</td>
            </tr>
          {% endif %}
          
          <tr class="bg-white"><td colspan="10" class="py-4"></td></tr>
          <tr class="border-t-4 border-gray-800 bg-gray-200 font-extrabold">
            <td colspan="5" class="py-3 px-2 text-xl">TOTAL DUE</td>
            <td colspan="4"></td>
            <td class="py-3 px-2 text-right text-xl">R{{ "%.2f"|format(grand_total) }}</td>
          </tr>
        </tbody>
      </table>'''

text = text.replace(old_total_row, new_total_row)
text = text.replace(old_total_row_alt, new_total_row)

# The total sentence at the top:
text = text.replace('Your property owes R{{ "%.2f"|format(grand_total|float) }} for Utilities.', 
                    'Statement amount: R{{ "%.2f"|format(grand_total|float) }}')

# Email endpoint
text = text.replace("{{ url_for('billing_bp.email_metsoa', property_id=property.id, month=month) }}",
                    "{{ url_for('billing_bp.email_soa', tenant_id=tenant.id, month=month) }}")

with open('templates/program_billing/soa_document.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Successfully created soa_document.html')
