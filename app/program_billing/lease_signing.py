"""One identity-bound signing-particulars map in the existing lease draft JSON."""
from copy import deepcopy
from datetime import date
from hashlib import sha256
import json,re
from .lease_certificate_parties import blocks,NATURAL,ENTITIES,identity_key
KEY='_signing_particulars'


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
            result.append(dict(party,key=key,role=group['label'],place=values.get('place',''),date=values.get('date','')))
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
            entry[field]=value
        saved[key]=entry
    result[KEY]=saved
    return result
