"""Read-only PM tenant-account ledger. Signed rand amounts, no materialisation."""
from decimal import Decimal,InvalidOperation
from flask import abort
from sqlalchemy import text,inspect
from app.extensions import db
from .lease_soa_dates import effective_date


def transactions(tenant_id):
    catalog=inspect(db.session.connection())
    required={'id','tenant_id','txn_date','description','kind','amount'}
    columns={row['name'] for row in catalog.get_columns('bil_tenant_ledger')} if catalog.has_table('bil_tenant_ledger') else set()
    if not required<=columns:
        abort(503,description='The existing PM tenant ledger schema is unavailable. No financial changes were made.')
    # Existing PM SOA convention: stored signed rand amounts, not shared debtor cents.
    extras=",ref" if 'ref' in columns else ',NULL AS ref'
    extras+=",posting_notes" if 'posting_notes' in columns else ',NULL AS posting_notes'
    audit={'correction_of','correction_role','correction_actor','correction_recorded_at','correction_reason'}<=columns
    if audit:
        extras+=",correction_of,correction_role,correction_actor,correction_recorded_at,correction_reason"
        extras+=",EXISTS(SELECT 1 FROM bil_tenant_ledger c WHERE c.correction_of=bil_tenant_ledger.id) AS corrected"
        extras+=",(SELECT s.ref FROM bil_tenant_ledger s WHERE s.id=bil_tenant_ledger.correction_of AND s.tenant_id=bil_tenant_ledger.tenant_id) AS original_ref"
    else:
        extras+=",NULL AS correction_of,NULL AS correction_role,NULL AS correction_actor,NULL AS correction_recorded_at,NULL AS correction_reason,false AS corrected,NULL AS original_ref"
    rows=db.session.execute(text('''SELECT id,txn_date,description,kind,amount'''+extras+'''
        FROM bil_tenant_ledger WHERE tenant_id=:t ORDER BY txn_date ASC NULLS LAST,id ASC'''),dict(t=tenant_id)).mappings()
    balance=Decimal('0.00');result=[]
    for row in rows:
        try:
            amount=Decimal(str(row['amount']))
            if not amount.is_finite():raise InvalidOperation()
        except (InvalidOperation,ValueError,TypeError):
            abort(503,description='A PM ledger amount is invalid. The account cannot be totalled safely; no records were changed.')
        balance+=amount
        description='Reversal' if row['correction_role']=='reversal' or row['kind']=='correction_reversal' else 'Opening Balance' if row['kind']=='opening_adjustment' else row['description']
        reference=row['ref'] or ''
        if reference.startswith('PMPOST:'):reference=reference.partition(' | ')[2]
        result.append(dict(row,display_date=effective_date(row['txn_date']),
            display_description=description,display_reference=reference,
            debit=amount if amount>0 else Decimal('0.00'),
            credit=-amount if amount<0 else Decimal('0.00'),balance=balance))
    return result
