"""Durable substitute provenance in the existing tariff audit evidence field."""
import json
from datetime import date
PREFIX='AIT_CURRENT_RATE_SUBSTITUTE_V1:'

def reference(source,on,copied_on):
    return PREFIX+json.dumps(dict(source_tariff_id=source.id,source_effective_date=source.effective_date.isoformat(),effective_date=on.isoformat(),copied_on=copied_on.isoformat(),classification='current-rate substitute, not verified historical tariff'),sort_keys=True)

def metadata(value):
    if not isinstance(value,str) or not value.startswith(PREFIX):return None
    data=json.loads(value[len(PREFIX):])
    for key in ('source_effective_date','effective_date','copied_on'):date.fromisoformat(data[key])
    if not isinstance(data['source_tariff_id'],int):raise ValueError('Invalid substitute source tariff ID.')
    return data
