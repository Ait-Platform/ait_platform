import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''              {% if bank_account %}
              <div class="mt-4">
                  <h3 class="text-xs uppercase font-bold text-gray-500 tracking-wider mb-1">Payment Details / Bank Info:</h3>
                  {% if bank_account.raw_details %}
                  <p class="text-sm text-gray-700 whitespace-pre-line">{{ bank_account.raw_details }}</p>
                  {% else %}
                  <div class="text-sm text-gray-700">
                      <div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>
                      <div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>
                      <div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>
                      {% if bank_account.swift_code %}<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>{% endif %}
                  </div>
                  {% endif %}
              </div>
              {% endif %}
              
              {% if shop and shop.terms_and_conditions %}
              <div class="mt-8 pt-4 border-t border-gray-200">
                  <h3 class="text-xs uppercase font-bold text-gray-500 tracking-wider mb-2">Terms & Conditions</h3>
                  <div class="text-xs text-gray-600 whitespace-pre-line">{{ shop.terms_and_conditions }}</div>
              </div>
              {% endif %}
          </div>
          
          <div class="mt-8 text-xs text-gray-400 italic">E.&O.E.</div>

      </div>
  </div>'''

content = re.sub(
    r"\s*\{% if bank_account %\}.*?\{% endif %\}\s*</div>\s*</div>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
