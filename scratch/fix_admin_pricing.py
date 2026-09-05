import re

with open('app/admin/security/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the zero constraints in manage_pricing
old_logic = """                        if computed_zar < 3000:
                            local_cents = int(3000 / fx)
                            computed_zar = int(local_cents * fx)
                        p.local_amount_cents = local_cents
                        p.zar_amount_cents = computed_zar
                    else:
                        p.local_amount_cents = local_cents
                        p.zar_amount_cents = max(3000, computed_zar if 'computed_zar' in locals() else 3000)"""

new_logic = """                        if computed_zar < 3000:
                            local_cents = int(3000 / fx)
                            computed_zar = max(3000, int(local_cents * fx))
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(1, computed_zar)
                    else:
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(3000, computed_zar if 'computed_zar' in locals() else 3000)"""

content = content.replace(old_logic, new_logic)

with open('app/admin/security/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed admin pricing engine zero constraints")
