"""Effective database tariffs; missing historical components are never invented."""
from datetime import date
from decimal import Decimal,InvalidOperation
from types import SimpleNamespace
from sqlalchemy import text,inspect
from app.extensions import db

def select(billing_date,use_current=False):
    import re
    issues=[];catalog=inspect(db.session.connection())
    required={'id','utility_type','code','description','rate','block_start','block_end','reduction_factor','unit','effective_date'}
    if not catalog.has_table('bil_tariff') or not required<={c['name'] for c in catalog.get_columns('bil_tariff')}:
        return dict(electricity=None,water=[],sewerage=[],water_fixed=[],sewerage_fixed=[],water_surcharge=None,sewerage_surcharge=None,records=[]),['Applicable tariff records are unavailable.']
    from app.utils.tariff_versions import resolve,identity,billing_date as parse_billing_date
    from datetime import datetime
    from zoneinfo import ZoneInfo
    on=parse_billing_date(billing_date)
    current_on=datetime.now(ZoneInfo('Africa/Johannesburg')).date()
    categories={}
    if catalog.has_table('bil_tariff_verification') and {'tariff_id','municipal_category'}<={c['name'] for c in catalog.get_columns('bil_tariff_verification')}:
        categories={r['tariff_id']:r['municipal_category'] for r in db.session.execute(text('SELECT tariff_id,municipal_category FROM bil_tariff_verification')).mappings()}
    source=[]
    for record in db.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings():
        row=dict(record);row['code']=str(row.get('code') or '').strip()
        code=re.sub(r'[^a-z0-9]','',row['code'].lower())
        category=re.sub(r'[^a-z0-9]','',str(categories.get(row['id']) or '').lower())
        is_loss=code in ('wloss','waterlosslevy') or ((row.get('utility_type') or '').strip().lower()=='water_fixed' and category in ('waterloss','waterlosslevy','waterlossinsurance','waterlosslevyinsurance','waterlosscharge'))
        kind=(row.get('utility_type') or '').strip().lower()
        surcharge=None
        if kind in ('water_surcharge','sewerage_surcharge'):surcharge=kind
        elif category in ('waterinfrastructuresurcharge',):surcharge='water_surcharge'
        elif category in ('sewerageinfrastructuresurcharge','sewagedisposalinfrastructuresurcharge'):surcharge='sewerage_surcharge'
        elif 'surcharge' in code:
            if kind.startswith('water') or code.startswith('w'):surcharge='water_surcharge'
            elif kind in ('sewerage','sanitation','sewerage_fixed') or code.startswith('s'):surcharge='sewerage_surcharge'
        row['charge_component']='water_loss' if is_loss else surcharge
        source.append(row)
    def family(row):
        if row.get('charge_component')=='water_loss':return ('water_fixed','water_loss')
        if row.get('charge_component') in ('water_surcharge','sewerage_surcharge'):return (row['charge_component'],'infrastructure')
        utility,code=identity(row)
        return ({'sanitation':'sewerage','electrical':'electricity','sanitation_fixed':'sewerage_fixed'}.get(utility,utility),code)
    versions,version_issues=resolve(source,billing_date,key_function=family)
    issues.extend(version_issues)
    if use_current and on<current_on.replace(day=1):
        present={family(r) for r in versions}
        groups={}
        for row in source:groups.setdefault(family(row),[]).append(row)
        for key,group in groups.items():
            if key in present:continue
            # Missing coverage can use today's schedule only after explicit consent.
            # Invalid/ambiguous historical evidence is never silently replaced.
            try:historical=any(date.fromisoformat(str(r['effective_date']))<=on for r in group)
            except (ValueError,TypeError):continue
            if historical:continue
            current,problems=resolve(group,current_on,key_function=family)
            if problems:issues.extend(problems)
            elif current:versions.append(dict(current[0],historical_estimate=True))
    selected=[]
    for row in versions:
        try:
            rate=Decimal(str(row['rate']))
            if not rate.is_finite() or rate<0:raise ValueError()
        except (ValueError,InvalidOperation):issues.append('Tariff '+row['code']+' has an invalid rate.');continue
        row['rate']=rate;row['effective_date']=date.fromisoformat(str(row['effective_date']));selected.append(SimpleNamespace(**row))
    result=dict(electricity=None,water=[],sewerage=[],water_fixed=[],sewerage_fixed=[],water_surcharge=None,sewerage_surcharge=None,records=selected,current_tariff_date=current_on)
    result['sewerage_surcharge_applicable']=any(r.get('charge_component')=='sewerage_surcharge' for r in source)
    selected_families={family(vars(r)) for r in selected}
    missing_families={family(r):r for r in source if family(r) not in selected_families}
    result['unavailable_components']=[]
    for row in missing_families.values():
        reason='Tariff '+row['code']+': no applicable unambiguous rate on '+str(billing_date)+'. Rate unavailable.'
        issues.append(reason)
        result['unavailable_components'].append(dict(number='Applicable meters',component=row['code'],reason=reason,utility_type=row['utility_type']))
    electricity=[];seen_surcharges=set();invalid_sides=set()
    for row in selected:
        kind=(row.utility_type or '').strip().lower();code=row.code.lower()
        if row.charge_component=='water_loss':
            # Confirmed charge-code semantics override incomplete legacy type/unit labels.
            # The amount still comes only from its applicable, unambiguous database version.
            result['water_fixed'].append(row)
            continue
        if row.charge_component in ('water_surcharge','sewerage_surcharge'):
            key=row.charge_component
            side=key.split('_')[0]
            if key in seen_surcharges:
                issues.append('Multiple '+side+' surcharge tariffs require review.');result[key]=None
            elif (row.unit or '').strip().lower().replace(' ','') not in {'r/kl','zar/kl','perkl'}:issues.append('Tariff '+row.code+' needs verified per-kL units before its consumption surcharge can be applied.')
            else:result[key]=row.rate
            seen_surcharges.add(key)
        elif kind in ('electricity','electrical'):electricity.append(row)
        elif kind in ('water_fixed','sewerage_fixed','sanitation_fixed') or code in ('w_loss','w_mgt','s_refuse'):
            result['water_fixed' if kind=='water_fixed' or code in ('w_loss','w_mgt') else 'sewerage_fixed'].append(row)
        elif kind in ('water','sewerage','sanitation'):
            side='water' if kind=='water' else 'sewerage'
            try:
                start=Decimal(str(row.block_start));factor=Decimal(str(row.reduction_factor))
                if not start.is_finite() or start<0 or not factor.is_finite() or factor<=0:raise ValueError()
                if row.block_end is not None and (not Decimal(str(row.block_end)).is_finite() or Decimal(str(row.block_end))<start):raise ValueError()
                result[side].append(dict(start=start,label=row.description or row.code,multiplier=factor,rate=row.rate,code=row.code))
            except (ValueError,InvalidOperation):
                issues.append('Tariff '+row.code+' has missing or invalid band boundaries/daily allocation metadata.');invalid_sides.add(side)
    if not any(r.charge_component=='water_loss' for r in result['water_fixed']):issues.append('No verified effective W_LOSS fixed per-water-meter insurance levy is available.')
    if len(electricity)==1:result['electricity']=electricity[0].rate
    elif not electricity:issues.append('No electricity tariff is effective on or before '+str(billing_date)+'.')
    else:issues.append('Multiple applicable electricity tariffs require an explicit selection; no rate was guessed.')
    for side in ('water','sewerage'):
        if side in invalid_sides:result[side]=[]
        result[side].sort(key=lambda r:r['start'])
        if not result[side]:issues.append('No applicable '+side+' consumption bands for '+str(billing_date)+'.')
        elif result[side][0]['start']!=0 or len({r['start'] for r in result[side]})!=len(result[side]):
            issues.append('The '+side+' bands have ambiguous starting boundaries.');result[side]=[]
        if result[side+'_surcharge'] is None and (side=='water' or result['sewerage_surcharge_applicable']):issues.append('No applicable '+side+' surcharge tariff for '+str(billing_date)+'. Rate unavailable.')
        if not result[side+'_fixed']:issues.append('No applicable '+side+' fixed-charge records are available; no fallback levy was used.')
    return result,issues
