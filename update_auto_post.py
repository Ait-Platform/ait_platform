import sys
import re

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

helper_function = """
from datetime import date
from app.models.billing import BilTenantLedger

def _auto_post_to_ledger(tenant_id, month, grand_total, tenant):
    # 1. Post METSOA Total
    metsoa_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="METSOA-AUTO").first()
    if metsoa_entry:
        metsoa_entry.amount = grand_total
    else:
        metsoa_entry = BilTenantLedger(
            tenant_id=tenant_id,
            month=month,
            txn_date=date.today(),
            description="Utilities (METSOA)",
            kind="charge",
            amount=grand_total,
            ref="METSOA-AUTO"
        )
        db.session.add(metsoa_entry)

    # 2. Post Rent
    rent_amount = 0
    if tenant.leases:
        rent_amount = tenant.leases[0].rent_amount or 0
    if rent_amount > 0:
        rent_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="RENT-AUTO").first()
        if rent_entry:
            rent_entry.amount = rent_amount
        else:
            rent_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=date.today(),
                description="Monthly Rent",
                kind="charge",
                amount=rent_amount,
                ref="RENT-AUTO"
            )
            db.session.add(rent_entry)
            
    # 3. Post Arrears
    arrears_amount = 0
    if tenant.leases:
        arrears_amount = tenant.leases[0].tenant_arrears_total or 0
    if arrears_amount > 0:
        arrears_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="ARREARS-AUTO").first()
        if arrears_entry:
            arrears_entry.amount = arrears_amount
        else:
            arrears_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=date.today(),
                description="Arrears",
                kind="charge",
                amount=arrears_amount,
                ref="ARREARS-AUTO"
            )
            db.session.add(arrears_entry)

    db.session.commit()

@billing_bp.route('/metsoa/<int:tenant_id>/<month>', methods=['GET'])
"""

content = content.replace("@billing_bp.route('/metsoa/<int:tenant_id>/<month>', methods=['GET'])", helper_function, 1)

# Call it in metsoa
content = content.replace("""    grand_total = round(elec_total + water_total, 2)
    
    data = {""", """    grand_total = round(elec_total + water_total, 2)
    
    # Auto-post to ledger
    _auto_post_to_ledger(tenant_id, month, grand_total, tenant)
    
    data = {""")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Updates applied to routes.py")
