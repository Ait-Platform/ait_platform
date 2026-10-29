"""Read-only tenant statement projection from the existing signed-rand PM ledger."""
import calendar,re
from datetime import date
from decimal import Decimal
from flask import abort
from .lease_soa_dates import today
from .lease_soa_ledger import transactions


def period(args):
    month=args.get('month') or today().strftime('%Y-%m')
    try:
        if not re.fullmatch(r'\d{4}-\d{2}',month):raise ValueError()
        first=date.fromisoformat(month+'-01')
        last=date(first.year,first.month,calendar.monthrange(first.year,first.month)[1])
        values=[]
        for key,fallback in [('from',first),('to',last)]:
            value=args.get(key,'')
            if value and not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
            values.append(date.fromisoformat(value) if value else fallback)
        start,end=values
        if end<start:raise ValueError()
    except (ValueError,TypeError):abort(400,description='Choose a valid month and a From/To range with To on or after From.')
    return month,start,end


def statement(tenant_id,args):
    month,start,end=period(args);opening=Decimal('0.00');balance=Decimal('0.00');entries=[]
    for row in transactions(tenant_id):
        effective=row['display_date']
        if effective is None:
            abort(409,description='An existing transaction has no effective date. A period statement cannot be calculated safely until it is resolved through the controlled accounting workflow.')
        amount=row['debit']-row['credit']
        if effective<start:opening+=amount
        elif effective<=end:
            balance+=amount
            # Deliberately pass no actor, audit timestamp, reason, notes or technical linkage.
            entries.append(dict(date=effective,description=row['display_description'],reference=row['display_reference'],
                debit=row['debit'],credit=row['credit'],movement=balance))
    for row in entries:row['balance']=opening+row.pop('movement')
    return dict(month=month,start=start,end=end,opening=opening,entries=entries,
        debits=sum((r['debit'] for r in entries),Decimal('0.00')),
        credits=sum((r['credit'] for r in entries),Decimal('0.00')),closing=opening+balance)
