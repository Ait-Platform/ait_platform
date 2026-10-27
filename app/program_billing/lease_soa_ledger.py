"""Read-only PM tenant-account ledger. Signed rand amounts, no materialisation."""
from decimal import Decimal,InvalidOperation
from flask import abort
from sqlalchemy import text,inspect
from app.extensions import db


def transactions(tenant_id):
    catalog=inspect(db.session.connection())
    required={'id','tenant_id','txn_date','description','kind','amount'}
    if not catalog.has_table('bil_tenant_ledger') or not required<={row['name'] for row in catalog.get_columns('bil_tenant_ledger')}:
        abort(503,description='The existing PM tenant ledger schema is unavailable. No financial changes were made.')
    # Existing PM SOA convention: stored signed rand amounts, not shared debtor cents.
    rows=db.session.execute(text('''SELECT id,txn_date,description,kind,amount
        FROM bil_tenant_ledger WHERE tenant_id=:t ORDER BY txn_date ASC NULLS LAST,id ASC'''),dict(t=tenant_id)).mappings()
    balance=Decimal('0.00');result=[]
    for row in rows:
        try:
            amount=Decimal(str(row['amount']))
            if not amount.is_finite():raise InvalidOperation()
        except (InvalidOperation,ValueError,TypeError):
            abort(503,description='A PM ledger amount is invalid. The account cannot be totalled safely; no records were changed.')
        balance+=amount
        result.append(dict(row,debit=amount if amount>0 else Decimal('0.00'),
            credit=-amount if amount<0 else Decimal('0.00'),balance=balance))
    return result
