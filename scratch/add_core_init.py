with open('app/models/__init__.py', 'a', encoding='utf-8') as f:
    f.write("\nfrom .core import CoreOrganization, CoreOrganizationMember\n__all__.extend(['CoreOrganization', 'CoreOrganizationMember'])\n")
print("Added core models to __init__.py")
