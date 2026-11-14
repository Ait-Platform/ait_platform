"""Pure, shared Carpentry ledger presentation and exact payment validation."""
from decimal import Decimal, InvalidOperation, localcontext
from types import SimpleNamespace


def ledger_period(rows, start=None, end=None):
    balance=0;opening=0;visible=[]
    for row in sorted(rows,key=lambda row:(row.txn_date,row.id)):
        if end and row.txn_date>end:continue
        balance+=int(row.amount) if row.kind=='debit' else -int(row.amount)
        if start and row.txn_date<start:
            opening=balance
        else:
            row.running_balance=balance;visible.append(row)
    return visible,opening,balance


def payment_cents(raw):
    try:value=Decimal(raw)
    except (InvalidOperation,TypeError):raise ValueError('Enter a valid payment amount.') from None
    if not value.is_finite() or value<=0 or value>Decimal('99999999999999999999999999.99') or value!=value.quantize(Decimal('.01')):
        raise ValueError('Enter a positive payment with at most two decimal places.')
    return int(value*100)


def shop_statement_profile(shop):
    fields=('business_name','address','phone','email','logo_url','letterhead_url','use_custom_letterhead','bank_details','terms_and_conditions')
    return SimpleNamespace(**{field:getattr(shop,field,None) for field in fields})


def format_cents(value, grouped=False):
    with localcontext() as context:
        context.prec=max(50,len(str(abs(int(value))))+4)
        amount=Decimal(int(value))/Decimal(100)
        return format(amount,',.2f' if grouped else '.2f')
