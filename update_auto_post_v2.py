import sys

file_path = r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

import re

# Replace the auto post function
old_func_pattern = re.compile(r"def _auto_post_to_ledger.*?db\.session\.commit\(\)", re.DOTALL)

new_func = """def _auto_post_to_ledger(tenant_id, month, grand_total, tenant, elec_rows=None):
    from datetime import date, datetime
    # Determine dates
    try:
        y, m = map(int, month.split('-'))
        base_date = date(y, m, 1)
    except:
        base_date = date.today()
        
    metsoa_date = base_date
    if elec_rows:
        try:
            last_date_str = elec_rows[-1].get('new_date')
            if last_date_str:
                metsoa_date = datetime.strptime(last_date_str, "%Y/%m/%d").date()
        except Exception:
            pass

    # 1. Post Arrears (Opening Balance)
    arrears_amount = 0
    if tenant.leases:
        arrears_amount = tenant.leases[0].tenant_arrears_total or 0
    if arrears_amount > 0:
        arrears_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="ARREARS-AUTO").first()
        if arrears_entry:
            arrears_entry.amount = arrears_amount
            arrears_entry.description = "Opening Balance"
            arrears_entry.txn_date = base_date
        else:
            arrears_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=base_date,
                description="Opening Balance",
                kind="charge",
                amount=arrears_amount,
                ref="ARREARS-AUTO"
            )
            db.session.add(arrears_entry)

    # 2. Post Rent
    rent_amount = 0
    if tenant.leases:
        rent_amount = tenant.leases[0].rent_amount or 0
    if rent_amount > 0:
        rent_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="RENT-AUTO").first()
        if rent_entry:
            rent_entry.amount = rent_amount
            rent_entry.txn_date = base_date
        else:
            rent_entry = BilTenantLedger(
                tenant_id=tenant_id,
                month=month,
                txn_date=base_date,
                description="Monthly Rent",
                kind="charge",
                amount=rent_amount,
                ref="RENT-AUTO"
            )
            db.session.add(rent_entry)
            
    # 3. Post METSOA Total
    metsoa_entry = BilTenantLedger.query.filter_by(tenant_id=tenant_id, month=month, ref="METSOA-AUTO").first()
    if metsoa_entry:
        metsoa_entry.amount = grand_total
        metsoa_entry.txn_date = metsoa_date
    else:
        metsoa_entry = BilTenantLedger(
            tenant_id=tenant_id,
            month=month,
            txn_date=metsoa_date,
            description="Utilities (METSOA)",
            kind="charge",
            amount=grand_total,
            ref="METSOA-AUTO"
        )
        db.session.add(metsoa_entry)

    db.session.commit()"""

content = old_func_pattern.sub(new_func, content)

# Update function calls
content = content.replace("_auto_post_to_ledger(tenant_id, month, grand_total, tenant)", "_auto_post_to_ledger(tenant_id, month, grand_total, tenant, elec_rows)")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("Updated successfully")
