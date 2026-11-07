"""Day-proportional dated pricing using the existing METSOA builders."""
from datetime import date,timedelta
from decimal import Decimal
from types import SimpleNamespace

class IntervalUnavailable(ValueError):pass

def slices(previous,current,consumption,changes=()):
    # A reading measures usage after its opening date through its closing date.
    first=previous+timedelta(days=1);stop=current+timedelta(days=1)
    if stop<=first:raise IntervalUnavailable('Reading interval must contain at least one day.')
    boundaries={first,stop}
    for year in range(first.year,stop.year+1):
        boundary=date(year,7,1)
        if first<boundary<stop:boundaries.add(boundary)
    boundaries.update(d for d in changes if first<d<stop)
    ordered=sorted(boundaries);result=[];allocated=Decimal(0);total=(stop-first).days
    for a,b in zip(ordered,ordered[1:]):
        units=consumption-allocated if b==stop else consumption*Decimal((b-a).days)/Decimal(total)
        allocated+=units;result.append(dict(start=a,end=b-timedelta(days=1),days=(b-a).days,consumption=units))
    return result

def price(record,p,month,monthly,select,builders,changes):
    water=record.utility_type.lower()=='water';parts=[];used=[];pricing_issues=[]
    from .municipal_metsoa import applicable_tariff_records
    for part in slices(record.last_date,record.new_date,record.consumption,changes):
        rates,_=select(part['end'])
        required=('water','sewerage') if water else ('electricity',)
        unavailable=[key.replace('_',' ') for key in required if rates[key] is None or rates[key]==[]]
        if unavailable:
            raise IntervalUnavailable('Rate unavailable for '+', '.join(unavailable)+' consumption from '+str(part['start'])+' to '+str(part['end'])+'.')
        if water and any(sum((t['multiplier']*part['days'] for t in rates[side]),Decimal(0))<part['consumption'] for side in ('water','sewerage')):
            raise IntervalUnavailable('Tariff bands do not cover consumption from '+str(part['start'])+' to '+str(part['end'])+'; no excess rate was guessed.')
        if water:
            for key in ('water_surcharge','sewerage_surcharge'):
                if key=='sewerage_surcharge' and not rates.get('sewerage_surcharge_applicable',False):continue
                if rates[key] is None:pricing_issues.append('Rate unavailable for '+key.replace('_',' ')+' from '+str(part['start'])+' to '+str(part['end'])+'.')
        # Ignore unrelated utility/fixed-rate changes when deciding to split.
        used.extend(r for r in applicable_tariff_records(record.utility_type.lower(),rates['records']) if (r.utility_type or '').lower() not in ('water_fixed','sewerage_fixed','sanitation_fixed') and getattr(r,'charge_component',None) not in ('water_loss','property_management','property_refuse'))
        signature=repr(([(side,[(t['start'],t['multiplier'],t['rate']) for t in rates[side]]) for side in ('water','sewerage')],rates['water_surcharge'],rates['sewerage_surcharge']) if water else rates['electricity'])
        if parts and parts[-1]['signature']==signature:
            parts[-1].update(end=part['end'],days=parts[-1]['days']+part['days'],consumption=parts[-1]['consumption']+part['consumption'])
        else:parts.append(dict(part,rates=rates,signature=signature))
    elec_builder,water_builder=builders;result=None;allocations=[]
    for part in parts:
        rates=part['rates']
        segment=SimpleNamespace(**dict(vars(record),last_date=part['start']-timedelta(days=1),new_date=part['end'],days=part['days'],consumption=part['consumption']))
        variable=dict(rates,water_fixed=[],sewerage_fixed=[])
        rows,_=(water_builder if water else elec_builder)(p,month,source=[segment],tariffs=variable)
        line=rows[0]
        allocations.append(dict(start=part['start'],end=part['end'],days=part['days'],consumption=part['consumption'],rate=rates['electricity'] if not water else None))
        if water:
            if result is None:result=dict(line,ws_breakdown=[],sd_breakdown=[],ws_cost=Decimal(0),sd_cost=Decimal(0),total=Decimal(0))
            for side in ('ws','sd'):
                for charge in line[side+'_breakdown']:
                    result[side+'_breakdown'].append(dict(charge,prorated=len(parts)>1,desc=charge['desc']+(' ('+str(part['start'])+' to '+str(part['end'])+')' if len(parts)>1 else '')))
                result[side+'_cost']+=line[side+'_cost']
            result['total']+=line['total']
        else:
            if result is None:result=dict(line,due=Decimal(0))
            result['due']+=line['due']
    if water:
        fixed,fixed_used=fixed_only(record,p,month,monthly,builders)
        for side in ('ws','sd'):
            result[side+'_breakdown'].extend(fixed[side+'_breakdown']);result[side+'_cost']+=fixed[side+'_cost']
        result['total']+=fixed['total'];used.extend(fixed_used)
        result['summary']=dict(result['summary'],last_date=record.last_date.strftime('%Y/%m/%d'),new_date=record.new_date.strftime('%Y/%m/%d'),days=str(record.days)+' KL/day',consumption=record.consumption,last_read=record.last_read,new_read=record.new_read,avg=round(record.consumption/record.days,3))
    else:
        result.update(last_date=record.last_date.strftime('%Y/%m/%d'),new_date=record.new_date.strftime('%Y/%m/%d'),last_read=record.last_read,new_read=record.new_read,days=record.days,consumption=record.consumption,rate_segments=allocations if len(parts)>1 else [],rate=None if len(parts)>1 else result['rate'])
    return result,used,pricing_issues

def fixed_only(record,p,month,monthly,builders):
    blank=dict(monthly,water=[],sewerage=[],water_surcharge=None,sewerage_surcharge=None)
    fixed_record=SimpleNamespace(**dict(vars(record),consumption=Decimal(0),days=max(1,record.days)))
    rows,_=builders[1](p,month,source=[fixed_record],tariffs=blank)
    used=[r for key in ('water_fixed','sewerage_fixed') for r in monthly[key] if getattr(r,'charge_component',None) not in ('property_management','property_refuse') or record.meter_id==record.property_fee_meter_id]
    return rows[0],used
