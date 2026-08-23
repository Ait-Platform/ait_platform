import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# For download_pdf and email_document, active_shop is used
content = content.replace(
    'bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id).order_by(BusinessBankAccount.is_default.desc()).first()',
    'bank_account = BusinessBankAccount.query.filter_by(user_id=(active_shop.user_id if "active_shop" in locals() and active_shop else shop.user_id)).order_by(BusinessBankAccount.is_default.desc()).first()'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
