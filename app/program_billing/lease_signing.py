"""One identity-bound signing-particulars map in the existing lease draft JSON."""
from copy import deepcopy
from datetime import date
from hashlib import sha256
import json,re
from .lease_certificate_parties import blocks,NATURAL,ENTITIES,identity_key
from .lease_text import human
KEY='_signing_particulars'


def signing_statement(place='',value=''):
    """Format saved particulars only; missing data remains a handwriting space."""
    place=str(place or '').strip().upper() or '____________________'
    try:day=date.fromisoformat(str(value))
    except (ValueError,TypeError):return 'SIGNED at '+place+' on this the ____ day of ____________________ ______'
    suffix='th' if 11<=day.day%100<=13 else {1:'st',2:'nd',3:'rd'}.get(day.day%10,'th')
    months=('JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER')
    return 'SIGNED at '+place+' on this the '+str(day.day)+suffix+' day of '+months[day.month-1]+' '+str(day.year)


def party_key(role,party):
    kind=party.get('type','')
    kind='natural' if kind in NATURAL else 'entity' if kind in ENTITIES else kind
    identity=identity_key(party.get('identity_number'))
    register=party.get('register_id')
    if not identity and register is None:return None
    return sha256(json.dumps([role,kind,identity or 'register:'+str(register)],separators=(',',':')).encode()).hexdigest()


def rows(items,owners=(),tenants=()):
    saved=items.get(KEY,{})
    if not isinstance(saved,dict):saved={}
    result=[]
    for group in blocks(items,owners,tenants):
        for party in group['parties']:
            key=party_key(group['label'].lower(),party)
            values=saved.get(key,{}) if key else {}
            if not isinstance(values,dict):values={}
            result.append(dict(party,key=key,role=group['label'],place=human(values.get('place','')),date=values.get('date',''),
                statement=signing_statement(values.get('place',''),values.get('date','')),
                printed_name=', '.join(rep['name'] for rep in party.get('representatives',[]) if rep.get('name')) or party.get('name','')))
    return result


def update(items,posted,owners=(),tenants=()):
    result=deepcopy(items);saved=deepcopy(items.get(KEY,{}))
    if not isinstance(saved,dict):raise ValueError('Saved signing particulars require review.')
    allowed={row['key'] for row in rows(items,owners,tenants) if row['key']}
    for name in posted:
        if name.startswith(('signing_place_','signing_date_')) and name.split('_',2)[2] not in allowed:
            raise ValueError('The contracting parties changed. Reopen Signing Particulars before saving.')
    for key in allowed:
        entry=deepcopy(saved.get(key,{}))
        for field,prefix in (('place','signing_place_'),('date','signing_date_')):
            if prefix+key not in posted:continue
            value=posted[prefix+key].strip()
            if len(value)>200 or '\x00' in value or re.search(r'\[[^\]]+\]',value):raise ValueError('Enter valid signing particulars without placeholder labels.')
            if field=='date' and value:
                try:
                    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
                    date.fromisoformat(value)
                except ValueError:raise ValueError('Signing dates must use YYYY-MM-DD.') from None
            if field=='place':
                value=human(value)
                if re.search(r'\b(?:change financial particulars|financial particulars to financial)\b',value,re.I):raise ValueError('Enter only the actual signing place, without heading instructions.')
            entry[field]=value
        saved[key]=entry
    result[KEY]=saved
    return result


def projection(items,owners=(),tenants=()):
    keys=sorted(row['key'] for row in rows(items,owners,tenants) if row['key'])
    saved=items.get(KEY,{})
    return {'parties':keys,'values':{key:{field:saved.get(key,{}).get(field,'') for field in ('place','date')} for key in keys}}

def baseline(items,property_id,draft_id,owners=(),tenants=(),confirm=False):
    from flask import current_app
    from flask_login import current_user
    from itsdangerous import URLSafeTimedSerializer
    return URLSafeTimedSerializer(current_app.secret_key,salt='ait-lease-signing-baseline').dumps({'property':property_id,'draft':str(draft_id),'user':str(current_user.id),'signing':projection(items,owners,tenants),'confirm':confirm})

def check_baseline(token,items,property_id,draft_id,owners=(),tenants=(),confirmed=False):
    from flask import current_app
    from flask_login import current_user
    from itsdangerous import URLSafeTimedSerializer,BadSignature
    try:
        data=URLSafeTimedSerializer(current_app.secret_key,salt='ait-lease-signing-baseline').loads(token,max_age=28800)
    except BadSignature:return False
    return (data.get('property')==property_id and data.get('draft')==str(draft_id) and data.get('user')==str(current_user.id)
        and data.get('signing')==projection(items,owners,tenants) and (not data.get('confirm') or confirmed))


def signature_sections(parties,lessor=None):
    """One mandated Lessor section, without requiring every registered owner to sign."""
    from .lease_certificate_parties import representative,has_representative
    owners=[row for row in parties if row['role']=='Lessor']
    mandate=representative(lessor or {})
    representatives=[mandate] if has_representative(mandate) else []
    if not representatives:
        for owner in owners:
            for rep in owner.get('representatives',[]):
                if rep not in representatives:representatives.append(rep)
    matching=[row for row in owners if str(row.get('register_id'))==str((lessor or {}).get('owner_id'))] if (lessor or {}).get('owner_id') is not None else []
    dated=[row for row in owners if row.get('place') or row.get('date')]
    dates={(row.get('place',''),row.get('date','')) for row in dated}
    saved=matching[0] if len(matching)==1 else dated[0] if len(dates)==1 else {}
    lessor_section=dict(role='Lessor',name='; '.join(row.get('name','') for row in owners),identity_number='',
        representatives=representatives,statement=signing_statement(saved.get('place',''),saved.get('date','')),
        printed_name=', '.join(rep['name'] for rep in representatives if rep.get('name')),
        capacity=', '.join(dict.fromkeys(rep['capacity'] for rep in representatives if rep.get('capacity'))),
        heading='FOR AND ON BEHALF OF THE LESSOR',signer_label='Authorised Representative')
    lessees=[deepcopy(row) for row in parties if row['role']=='Lessee']
    first=lessees[0] if lessees else {}
    lessee_section=dict(role='Lessee',name='',identity_number='',representatives=[],
        statement=signing_statement(first.get('place',''),first.get('date','')),
        heading='LESSEE / AUTHORISED REPRESENTATIVE',signer_label='Lessee or Authorised Representative',signers=[])
    for row in lessees or [dict(name='',printed_name='',representatives=[])]:
        signer=dict(row,capacity=', '.join(dict.fromkeys(rep['capacity'] for rep in row.get('representatives',[]) if rep.get('capacity'))))
        signer['additional_particulars']=signing_statement(row.get('place',''),row.get('date','')).removeprefix('SIGNED at ') if (row.get('place'),row.get('date'))!=(first.get('place'),first.get('date')) else ''
        lessee_section['signers'].append(signer)
    lessor_section['signers']=[dict(printed_name=lessor_section['printed_name'],capacity=lessor_section['capacity'])]
    return [lessor_section,lessee_section]
