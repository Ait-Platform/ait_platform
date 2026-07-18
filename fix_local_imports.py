import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. manual_capture
mc_sig = "def manual_capture():"
mc_imp = "def manual_capture():\n    from app.models.billing import BilArchitectureDraft"
content = content.replace(mc_sig, mc_imp)

# 2. save_architecture_draft
sad_sig = "def save_architecture_draft(property_id):"
sad_imp = "def save_architecture_draft(property_id):\n    from app.models.billing import BilArchitectureDraft"
content = content.replace(sad_sig, sad_imp)

# 3. save_global_architecture
sga_sig = "def save_global_architecture(property_id):"
sga_imp = "def save_global_architecture(property_id):\n    from app.models.billing import BilArchitectureDraft"
content = content.replace(sga_sig, sga_imp)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Injected local imports.")
