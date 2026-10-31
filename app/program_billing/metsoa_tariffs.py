"""Read-only, date-effective tariff selection. Never supplies legacy fallback rates."""
from datetime import date
from decimal import Decimal,InvalidOperation
from types import SimpleNamespace
from sqlalchemy import text,inspect
from app.extensions import db

def select(billing_date):
    issues=[];catalog=inspect(db.session.connection())
    required={'id','utility_type','code','description','rate','block_start','block_end','reduction_factor','unit','effective_date'}
    if not catalog.has_table('bil_tariff') or not required<={c['name'] for c in catalog.get_columns('bil_tariff')}:
        return dict(electricity=None,water=[],sewerage=[],water_fixed=[],sewerage_fixed=[],water_surcharge=None,sewerage_surcharge=None,records=[]),['Applicable tariff records are unavailable.']
    versions={}
    for row in db.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings():
        row=dict(row);code=row.get('code') or '(unnamed)'
        try:effective=date.fromisoformat(str(row['effective_date']))
        except (ValueError,TypeError):issues.append('Tariff '+code+' has an invalid effective date.');continue
        if effective>billing_date:continue
        key=((row.get('utility_type') or '').lower(),code);versions.setdefault(key,[]).append((effective,row))
    selected=[]
    for key,entries in versions.items():
        latest=max(e for e,r in entries);rows=[r for e,r in entries if e==latest]
        if len(rows)!=1:issues.append('Tariff '+key[1]+' has conflicting records for '+str(latest)+'.');continue
        row=rows[0]
        try:
            rate=Decimal(str(row['rate']))
            if not rate.is_finite() or rate<0:raise ValueError()
        except (ValueError,InvalidOperation):issues.append('Tariff '+key[1]+' has an invalid rate.');continue
        row['rate']=rate;row['effective_date']=latest;selected.append(SimpleNamespace(**row))
    result=dict(electricity=None,water=[],sewerage=[],water_fixed=[],sewerage_fixed=[],water_surcharge=None,sewerage_surcharge=None,records=selected)
    electricity=[]
    for row in selected:
        kind=row.utility_type.lower();code=row.code.lower()
        if 'surcharge' in code or code in ('w_surcharge','s_surcharge'):
            side='water' if kind.startswith('water') or code.startswith('w') else 'sewerage' if kind in ('sewerage','sanitation','sewerage_fixed') or code.startswith('s') else None
            if side:
                key=side+'_surcharge'
                if result[key] is not None:issues.append('Multiple '+side+' surcharge tariffs require review.')
                else:result[key]=row.rate
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
            except (ValueError,InvalidOperation):issues.append('Tariff '+row.code+' has missing or invalid band boundaries/daily allocation metadata.')
    if len(electricity)==1:result['electricity']=electricity[0].rate
    elif not electricity:issues.append('No electricity tariff is effective on or before '+str(billing_date)+'.')
    else:issues.append('Multiple applicable electricity tariffs require an explicit selection; no rate was guessed.')
    for side in ('water','sewerage'):
        result[side].sort(key=lambda r:r['start'])
        if not result[side]:issues.append('No applicable '+side+' consumption bands for '+str(billing_date)+'.')
        elif result[side][0]['start']!=0 or len({r['start'] for r in result[side]})!=len(result[side]):
            issues.append('The '+side+' bands have ambiguous starting boundaries.');result[side]=[]
        if result[side+'_surcharge'] is None:issues.append('No applicable '+side+' surcharge tariff is recorded; the legacy R1.48 fallback was not used.')
        if not result[side+'_fixed']:issues.append('No applicable '+side+' fixed-charge records are available; no fallback levy was used.')
    return result,issues
