import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add the import to line 38
old_import = "from app.models.billing import BilStatementPayment, BilProperty, BilSectionalUnit, BilMeter, BilPlatformSettings"
new_import = "from app.models.billing import BilStatementPayment, BilProperty, BilSectionalUnit, BilMeter, BilPlatformSettings, BilArchitectureDraft"

if old_import in content:
    content = content.replace(old_import, new_import)
else:
    # Just insert it at the top
    content = "from app.models.billing import BilArchitectureDraft\n" + content

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Imported BilArchitectureDraft.")

# Also wait! BilArchitectureDraft is a new table. Did I create the table in SQLite?
# No! I only added the model to `billing.py`.
# I need to run db.create_all() so the table actually exists in data.db!
