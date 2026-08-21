import re

with open('templates/program_mechanic/invoice_view.html', 'r', encoding='utf-8') as f:
    content = f.read()

header_original = '''      {% if shop.use_custom_letterhead %}
        <div class="hidden print:block h-48 w-full"></div>
      {% elif shop.letterhead_url %}
        <div class="w-full mb-8">
          <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url) }}" alt="Shop Letterhead" class="w-full object-cover">
        </div>
      {% endif %}
      
      <div class="flex justify-between items-start {% if shop.use_custom_letterhead or shop.letterhead_url %}hidden{% endif %}">'''

header_new = '''      {% if shop.use_custom_letterhead and not is_email %}
        <div class="hidden print:block h-48 w-full"></div>
      {% elif shop.letterhead_url %}
        <div class="w-full mb-8">
          <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url) }}" alt="Shop Letterhead" class="w-full object-cover">
        </div>
      {% endif %}
      
      <div class="flex justify-between items-start {% if (shop.use_custom_letterhead and not is_email) or shop.letterhead_url %}hidden{% endif %}">'''

content = content.replace(header_original, header_new)

# Update document title logic in invoice_view.html to support "Quote / Tax Invoice"
title_original = '''        <h1 class="text-2xl font-bold text-slate-900">Invoice: {{ job_card.job_number }}</h1>'''
title_new = '''        <h1 class="text-2xl font-bold text-slate-900">
          {% if job_card.status == 'Quote' %}Quote / Tax Invoice
          {% elif job_card.status in ['Approved', 'Billed'] %}Tax Invoice
          {% else %}Job Card{% endif %}: {{ job_card.job_number }}
        </h1>'''
content = content.replace(title_original, title_new)

doc_header_original = '''          <h2 class="text-3xl font-black text-indigo-700 uppercase tracking-widest text-right">
            {% if job_card.status == 'Quote' %}QUOTATION
            {% elif job_card.status == 'Billed' %}TAX INVOICE
            {% else %}JOB CARD
            {% endif %}
          </h2>'''

doc_header_new = '''          <h2 class="text-3xl font-black text-indigo-700 uppercase tracking-widest text-right">
            {% if job_card.status == 'Quote' %}QUOTE / TAX INVOICE
            {% elif job_card.status in ['Approved', 'Billed'] %}TAX INVOICE
            {% else %}JOB CARD
            {% endif %}
          </h2>'''
content = content.replace(doc_header_original, doc_header_new)

with open('templates/program_mechanic/invoice_view.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated invoice_view.html")
