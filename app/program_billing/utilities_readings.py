"""Existing meter reading/consumption capture with property scope and append-only history."""
from datetime import date
from decimal import Decimal,InvalidOperation
from flask import abort
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def number(value):
    try:
        parsed=Decimal(str(value))
        if not parsed.is_finite() or parsed<0 or parsed>Decimal('1000000000000'):raise ValueError()
        return parsed
    except (ValueError,InvalidOperation,TypeError):abort(400,description='Enter a finite, non-negative reading or consumption amount.')

def save(data):
    if not isinstance(data,dict):abort(400)
    meter=db.session.execute(text('SELECT id,property_id,meter_number FROM bil_meter WHERE id=:id'),dict(id=data.get('meter_id'))).mappings().first()
    if not meter:abort(404)
    if not meter['property_id']:abort(409,description='This historical meter has no verified property association.')
    property_scope(meter['property_id'],lock=True)
    if data.get('property_id') and str(data['property_id'])!=str(meter['property_id']):abort(403)
    try:
        new_date=date.fromisoformat(data.get('date',''))
        period=data['reading_month']
        if new_date.strftime('%Y-%m')!=period:raise ValueError()
    except (ValueError,TypeError,KeyError):abort(400,description='Reading date must fall within the selected billing month.')
    estimate=data.get('estimated_consumption')
    value=number(data.get('reading')) if estimate is None else Decimal(0)
    previous=db.session.execute(text('SELECT reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date<:d ORDER BY reading_date DESC,id DESC LIMIT 1'),dict(m=meter['id'],d=new_date)).mappings().first()
    if estimate is not None:
        prior=Decimal(0);prior_date=new_date;consumption=number(estimate);days=30
    else:
        prior=number(data['prev_reading']) if data.get('prev_reading') not in (None,'') else number(previous['reading_value']) if previous else Decimal(0)
        try:prior_date=date.fromisoformat(data['prev_date']) if data.get('prev_date') else previous['reading_date'] if previous else new_date.replace(day=1)
        except (ValueError,TypeError):abort(400,description='Invalid previous reading date.')
        if prior_date>=new_date or value<prior:abort(400,description='Reading must not decrease and its date must follow the previous reading date.')
        days=(new_date-prior_date).days;consumption=value-prior
    existing=db.session.execute(text('SELECT * FROM bil_consumption WHERE meter_id=:m AND month=:period'),dict(m=meter['id'],period=period)).mappings().all()
    if existing:
        if len(existing)==1 and existing[0]['new_date']==new_date and number(existing[0]['new_read'])==value and number(existing[0]['consumption'])==consumption:
            return dict(success=True,message='This reading was already saved; history is unchanged.')
        abort(409,description='Consumption already exists for this meter and month. Historical records were preserved; use a separately reviewed correction process.')
    if estimate is None:
        same=db.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date=:d'),dict(m=meter['id'],d=new_date)).scalars().all()
        if same and any(number(v)!=value for v in same):abort(409,description='A different reading already exists for this date.')
        if not same:db.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter['id'],d=new_date,v=value))
    db.session.execute(text('INSERT INTO bil_consumption(meter_id,meter_number,month,last_date,new_date,last_read,new_read,days,consumption) VALUES(:m,:n,:period,:ld,:nd,:lr,:nr,:days,:c)'),dict(m=meter['id'],n=meter['meter_number'],period=period,ld=prior_date,nd=new_date,lr=prior,nr=value,days=days,c=consumption))
    return dict(success=True,message='Reading and consumption saved.')
