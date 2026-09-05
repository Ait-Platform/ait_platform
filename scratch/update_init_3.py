import re

with open('app/models/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_import = "from .core import CoreOrganization, CoreOrganizationMember, CoreRole, CorePermission, CoreRolePermission, CoreRoleAssignment"
new_import = "from .core import CoreOrganization, CoreOrganizationMember, CoreRole, CorePermission, CoreRolePermission, CoreRoleAssignment, CoreInteraction, CoreTask"

old_export = "__all__.extend(['CoreOrganization', 'CoreOrganizationMember', 'CoreRole', 'CorePermission', 'CoreRolePermission', 'CoreRoleAssignment'])"
new_export = "__all__.extend(['CoreOrganization', 'CoreOrganizationMember', 'CoreRole', 'CorePermission', 'CoreRolePermission', 'CoreRoleAssignment', 'CoreInteraction', 'CoreTask'])"

text = text.replace(old_import, new_import)
text = text.replace(old_export, new_export)

with open('app/models/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated __init__.py with Phase 3 models")
