import re

init_path = 'app/uip/__init__.py'
with open(init_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update patches
old_patches = '''        ("core_organization_member", "joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("core_organization_member", "left_at TIMESTAMP")
    ]'''

new_patches = '''        ("core_organization_member", "joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("core_organization_member", "left_at TIMESTAMP"),
        ("core_interaction", "assigned_to INTEGER"),
        ("core_interaction", "closed_by INTEGER"),
        ("core_interaction", "channel VARCHAR(50)"),
        ("core_interaction", "category VARCHAR(100)")
    ]'''

text = text.replace(old_patches, new_patches)

# 2. Add dummy query to trigger patch
old_auth = '''        if current_user.is_authenticated:
            try:
                membership = CoreOrganizationMember.query.filter_by('''

new_auth = '''        if current_user.is_authenticated:
            # Force schema check on CoreInteraction so it auto-patches if missing
            from app.models.core import CoreInteraction
            try:
                CoreInteraction.query.filter_by(organization_id=org.id).first()
            except ProgrammingError as e:
                db.session.rollback()
                if "does not exist" in str(e) or "UndefinedColumn" in str(e):
                    auto_patch_database()
                else:
                    raise

            try:
                membership = CoreOrganizationMember.query.filter_by('''

text = text.replace(old_auth, new_auth)

with open(init_path, 'w', encoding='utf-8') as f:
    f.write(text)
