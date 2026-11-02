"""Read-only tariff version resolution; no price, factor or tier calculations."""
from calendar import monthrange
from datetime import date,datetime
from sqlalchemy import text
from werkzeug.exceptions import UnprocessableEntity

class TariffSelectionError(UnprocessableEntity):
    description='Applicable tariff versions could not be verified.'

def billing_date(value):
    if isinstance(value,datetime):return value.date()
    if isinstance(value,date):return value
    value=str(value or '')
    try:
        if len(value)==7:
            year,month=map(int,value.split('-'));return date(year,month,monthrange(year,month)[1])
        parsed=date.fromisoformat(value)
        if parsed.isoformat()!=value:raise ValueError()
        return parsed
    except (ValueError,TypeError):raise TariffSelectionError(description='Select a valid billing date or billing month.')

def identity(row):
    return (str(row.get('utility_type') or '').strip().lower(),str(row.get('code') or '').strip().casefold())

def resolve(rows,on,key_function=None):
    """Latest start <= billing date, one row per (utility type, code).

    Existing start-only records supersede earlier starts of the same identity.
    Equal starts are ambiguous, never a tie broken using row ID or price.
    Dates are validated before comparison; missing dates cannot be positioned.
    """
    on=billing_date(on);groups={};issues=[]
    for original in rows:
        row=dict(original);key=(key_function or identity)(row)
        if not all(key):issues.append('Tariff row '+str(row.get('id','unknown'))+' has no verified utility type or code.');continue
        groups.setdefault(key,[]).append(row)
    result=[]
    for key,versions in groups.items():
        dated=[];invalid=False
        for row in versions:
            raw=row.get('effective_date')
            try:
                effective=date.fromisoformat(str(raw))
                if effective.isoformat()!=str(raw):raise ValueError()
            except (ValueError,TypeError):
                issues.append('Tariff '+str(row.get('code'))+' row '+str(row.get('id','unknown'))+' has a missing or invalid effective date.');invalid=True;continue
            if effective<=on:dated.append((effective,row))
        if invalid:continue
        # Duplicate effective starts would represent overlapping versions in this start-only schema.
        starts={}
        for effective,row in dated:starts.setdefault(effective,[]).append(row)
        duplicate=next((d for d,items in sorted(starts.items()) if len(items)>1),None)
        if duplicate is not None:
            issues.append('Tariff '+str(versions[0]['code'])+' has conflicting records effective '+duplicate.isoformat()+'.');continue
        if dated:result.append(max(dated,key=lambda item:item[0])[1])
    return result,issues

def rows(session,on,utility=None,code=None):
    """Strict legacy adapter, preserving original row types and attributes."""
    source=[dict(row) for row in session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
    if utility is not None:
        allowed={str(u).strip().lower() for u in (utility if isinstance(utility,(tuple,list,set)) else (utility,))}
        source=[r for r in source if identity(r)[0] in allowed]
    if code is not None:source=[r for r in source if identity(r)[1]==str(code).strip().casefold()]
    selected,issues=resolve(source,on)
    if issues:raise TariffSelectionError(description=' '.join(issues))
    selected_keys={identity(row) for row in selected}
    unavailable={identity(row) for row in source}-selected_keys
    if unavailable and (code is not None or not selected):raise TariffSelectionError(description='No tariff version is effective on '+billing_date(on).isoformat()+' for '+', '.join(key[1] for key in sorted(unavailable))+'.')
    if code is not None and len(selected)>1:raise TariffSelectionError(description='Tariff code '+str(code)+' belongs to multiple utility identities; specify the utility type.')
    return selected

def one(session,on,code,utility=None):
    selected=rows(session,on,utility=utility,code=code)
    return selected[0] if selected else None

def legacy_row(row):
    if row is None:return None
    return dict(row,reduction_factor=row['reduction_factor'] if row.get('reduction_factor') is not None else 1,unit=row.get('unit') or '')

def by_code(session,on):
    result={}
    for row in rows(session,on):
        if row['code'] in result:raise TariffSelectionError(description='Tariff code '+row['code']+' has multiple utility identities.')
        result[row['code']]=legacy_row(row)
    return result

def only(session,on,utility):
    selected=rows(session,on,utility=utility)
    if len(selected)>1:raise TariffSelectionError(description='Multiple applicable '+str(utility)+' tariff identities require an explicit selection.')
    return selected[0] if selected else None
