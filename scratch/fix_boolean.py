import re

with open('migrations/versions/7da57fffdba9_sync_db_to_models.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix active::boolean
old_active = """    op.alter_column('auth_approved_admin', 'active',
               existing_type=sa.INTEGER(),
               server_default=None,
               type_=sa.Boolean(),
               nullable=True)"""

new_active = """    op.alter_column('auth_approved_admin', 'active',
               existing_type=sa.INTEGER(),
               server_default=None,
               type_=sa.Boolean(),
               postgresql_using='active::boolean',
               nullable=True)"""

text = text.replace(old_active, new_active)

with open('migrations/versions/7da57fffdba9_sync_db_to_models.py', 'w', encoding='utf-8') as f:
    f.write(text)
