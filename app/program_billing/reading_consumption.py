"""Refresh only consumption derived from canonical saved readings of the changed meter."""
from decimal import Decimal
from datetime import date
from sqlalchemy import text
from .municipal_metsoa import month_context,reading_pair_from_rows

FIELDS=('last_date','new_date','last_read','new_read','days','consumption')


def derived(meter,period,rows):
    _,start,end=month_context(period)
    pair=reading_pair_from_rows(meter,start,end,rows)
    if pair['error']:return None
    return dict(last_date=pair['previous']['date'],new_date=pair['current']['date'],last_read=pair['previous']['value'],new_read=pair['current']['value'],days=pair['days'],consumption=pair['difference'])


def matches(row,values):
    if values is None:return False
    return all(row.get(k) is not None and (Decimal(str(row[k]))==values[k] if k in ('last_read','new_read','consumption') else row[k]==values[k]) for k in FIELDS)


def from_saved_readings(row,rows):
    try:
        start=row['last_date'];end=row['new_date'];old=Decimal(str(row['last_read']));new=Decimal(str(row['new_read']))
        return start<end and end.strftime('%Y-%m')==row['month'] and row['days']==(end-start).days and Decimal(str(row['consumption']))==new-old and all(any(r['reading_date']==d and Decimal(str(r['reading_value']))==v for r in rows) for d,v in [(start,old),(end,new)])
    except (TypeError,ValueError,ArithmeticError):return False


def refresh(session,meter,before_rows,after_rows,changed_periods):
    stored=[dict(r) for r in session.execute(text('SELECT * FROM bil_consumption WHERE meter_id=:m ORDER BY id FOR UPDATE'),dict(m=meter['id'])).mappings()]
    groups={}
    for row in stored:groups.setdefault(row['month'],[]).append(row)
    changes=[]
    periods=[]
    for period in set(groups)|set(changed_periods):
        try:
            if not isinstance(period,str) or len(period)!=7:continue
            date.fromisoformat(period+'-01')
        except ValueError:continue
        periods.append(period)
    for period in sorted(periods):
        # Leave legacy/estimated consumption with no verifiable reading provenance untouched.
        try:old=derived(meter,period,before_rows);new=derived(meter,period,after_rows)
        except (ValueError,TypeError):continue
        existing=groups.get(period,[])
        for row in existing:
            if not from_saved_readings(row,before_rows) or matches(row,new):continue
            if old==new and from_saved_readings(row,after_rows):continue
            if new is None:
                session.execute(text('DELETE FROM bil_consumption WHERE id=:id AND meter_id=:m'),dict(id=row['id'],m=meter['id']))
                changes.append((row['id'],row,dict(invalidated=True,reason='No unambiguous saved reading pair remains for this month.')))
            else:
                session.execute(text('UPDATE bil_consumption SET last_date=:last_date,new_date=:new_date,last_read=:last_read,new_read=:new_read,days=:days,consumption=:consumption WHERE id=:id AND meter_id=:m'),dict(new,id=row['id'],m=meter['id']))
                changes.append((row['id'],row,dict(row,**new)))
        if not existing and new is not None and period in changed_periods:
            ident=session.execute(text('INSERT INTO bil_consumption(meter_id,meter_number,month,last_date,new_date,last_read,new_read,days,consumption) VALUES(:m,:number,:period,:last_date,:new_date,:last_read,:new_read,:days,:consumption) RETURNING id'),dict(new,m=meter['id'],number=meter['meter_number'],period=period)).scalar_one()
            changes.append((ident,None,dict(new,id=ident,meter_id=meter['id'],meter_number=meter['meter_number'],month=period)))
    return changes
