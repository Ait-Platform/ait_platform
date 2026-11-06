"""Inactive recovered rates mapped to existing tariff families; no calculator or writes."""
import json,re
from pathlib import Path
from datetime import date
from decimal import Decimal

TRANSITION='2024-11-01'

def catalogue():
    return json.loads(Path(__file__).with_name('recovered_almond_tariffs.json').read_text(encoding='utf-8'))

def normal(value):return re.sub(r'[^a-z0-9]','',str(value or '').lower())

def target_component(variable):
    if variable=='EleRate':return 'electricity',None
    if variable.startswith('WsRate'):return 'water',Decimal(variable[6:])
    if variable.startswith('SdRate'):return 'sewerage',Decimal(variable[6:])
    return {'WtrSurcharge':('water_surcharge',None),'WtrLossLevy':('water_loss',None),
            'SDSurcharge':('sewerage_surcharge',None),'SdBinCharge':('refuse',None)}.get(variable)

def matching(record,component,categories):
    kind=(record.get('utility_type') or '').lower();code=normal(record.get('code'))
    category=normal(categories.get(record['id']))
    utility,upper=component
    if upper is not None:
        if kind not in (('water',) if utility=='water' else ('sewerage','sanitation')) or 'surcharge' in code:return False
        # The established final S04 band may already be unbounded. Keep it so; do not invent a boundary.
        if utility=='sewerage' and upper==1500 and record.get('block_end') is None and code in ('s04','tier4sd'):return True
        try:return Decimal(str(record.get('block_end')))==upper
        except ArithmeticError:return False
    if utility=='electricity':return kind in ('electricity','electrical')
    if utility in ('water_surcharge','sewerage_surcharge'):
        if kind==utility:return True
        return code in (('wssurcharge','wsurcharge') if utility=='water_surcharge' else ('sdsurcharge','ssurcharge'))
    if utility=='water_loss':return code in ('wloss','waterlosslevy') or (kind=='water_fixed' and category in ('waterloss','waterlosslevy','waterlossinsurance','waterlosslevyinsurance','waterlosscharge'))
    return code in ('srefuse','refusebin') or (kind in ('sewerage_fixed','sanitation_fixed') and category in ('refuse','refusebins','refusebincharge'))

def review(records,categories=None):
    categories=categories or {};result=[]
    for schedule in catalogue()['schedules']:
        rows=[]
        for variable,literal in schedule['values'].items():
            row=dict(variable=variable,literal=literal,target=None,ready=False,note='No verified equivalent in the existing AIT components. Not prepared for activation.')
            component=target_component(variable)
            if variable.startswith('Unit'):row['note']='Historical tier setting retained as supplied. Existing AIT boundaries and calculation factors are not changed.'
            elif variable.startswith('Tier'):row['note']='Percentage basis unresolved. Not copied into reduction_factor; the existing calculator remains unchanged.'
            elif variable=='MthMgtFee14':row['note']='Monthly management-fee basis and suffix 14 are unresolved. Not substituted for W_MGT / MgmtFee.'
            if component:
                matches=[r for r in records if matching(r,component,categories)]
                # Prefer the established dedicated surcharge family over a legacy alias.
                dedicated=[r for r in matches if r['utility_type']==component[0]] if component[0].endswith('_surcharge') else []
                if dedicated:matches=dedicated
                families={(r['utility_type'],r['code']) for r in matches}
                if len(families)==1:
                    latest=max(matches,key=lambda r:(str(r.get('effective_date') or ''),r['id']))
                    row.update(target=dict(latest),ready=True,note='Existing AIT component; only its rate and explicit Effective From date are prepared. Units, tiers and factors stay unchanged.')
                    if component[0].endswith('_surcharge'):
                        if normal(latest.get('unit')) not in ('rkl','zarkl','perkl'):
                            row.update(ready=False,note='Existing surcharge units are unverified; no unit or calculation basis is changed.')
                        elif variable=='SDSurcharge':row['note']='Uses the existing active AIT sewerage-surcharge calculation (applicable kL ? rate), not a reconstructed VB6 formula.'
                    if schedule['id']=='A' and not any(r.get('effective_date')==TRANSITION for r in matches):row.update(ready=False,note='Schedule A start date is unknown. Save/review the mapped 1 November 2024 boundary version first so A cannot run into Schedule B.')
                    start=schedule['stated_start']
                    later=sorted(str(r['effective_date']) for r in matches if r.get('effective_date') and (not start or str(r['effective_date'])>start))
                    row['next_start']=later[0] if later else None
                    if start and not later:row.update(ready=False,note='No later stored version bounds this historical rate. It is not prepared because it could replace the current schedule.')
                    if start and any(r.get('effective_date')==start for r in matches):row.update(ready=False,note='A version already exists at the stated transition. It is preserved; no duplicate or overwrite is prepared.')
                elif matches:row['note']='Multiple existing tariff families match. No family is selected or duplicated automatically.'
                else:row['note']='No matching existing tariff component. No new category, missing tier or rate is invented.'
            rows.append(row)
        result.append(dict(schedule,rows=rows))
    return result

def selected_schedule(month):
    day=date.fromisoformat(month+'-01')
    return 'A' if day<date.fromisoformat(TRANSITION) else 'B'

def prepared(records,categories,schedule_id,variable,tariff_id):
    schedules=review(records,categories)
    schedule=next((s for s in schedules if s['id']==schedule_id),None)
    row=next((r for r in schedule['rows'] if r['variable']==variable),None) if schedule else None
    if not row or not row['ready'] or row['target']['id']!=int(tariff_id):raise ValueError('This recovered value has no unambiguous, eligible existing tariff target. Review the recovered schedules again.')
    reference='Recovered Almond VB6 settings; Schedule '+schedule_id+'; '+variable+'='+row['literal']+'. Applicability and official source require administrator verification.'
    return dict(rate=row['literal'],effective_date=schedule['stated_start'] or '',description=row['target'].get('description') or '',source_reference=reference,base_id=str(row['target']['id'])),schedule,row
