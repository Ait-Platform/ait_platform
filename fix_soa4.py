import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''  <!-- Email SOA Modal -->'''
new_target = '''  {% if not is_pdf %}
  <!-- Email SOA Modal -->'''
content = content.replace(target, new_target)

target_end = '''{% if is_pdf %}
</body>
</html>
{% endif %}'''
new_target_end = '''{% endif %}
{% if is_pdf %}
</body>
</html>
{% endif %}'''
content = content.replace(target_end, new_target_end)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("soa_template.html fixed 4!")
