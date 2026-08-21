import sys
with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<!-- Email SOA Modal -->''',
    '''{% if not is_pdf %}
  <!-- Email SOA Modal -->'''
)

content = content.replace(
    '''    </div>
  </div>
</div>

{% if is_pdf %}
</body>''',
    '''    </div>
  </div>
</div>
{% endif %}
{% if is_pdf %}
</body>'''
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
