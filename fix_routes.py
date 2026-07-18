
with open('app/admin/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'enquiry_cents = request.form.get("practice_enquiry_cents")',
    'enquiry_cents = request.form.get("practice_enquiry_cents")\n        billing_cents = request.form.get("billing_statement_cents")'
)

content = content.replace(
    'db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = \'practice_enquiry_cents\'"), {"val": enquiry_cents})',
    'db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = \'practice_enquiry_cents\'"), {"val": enquiry_cents})\n        if billing_cents:\n            db.session.execute(text("UPDATE system_settings SET value = :val, updated_at = CURRENT_TIMESTAMP WHERE key = \'billing_statement_cents\'"), {"val": billing_cents})'
)

with open('app/admin/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Updated app/admin/routes.py')
