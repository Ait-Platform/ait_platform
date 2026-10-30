"""Read-only bulk reconciliation; existing tariff functions remain authoritative."""
from decimal import Decimal
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def reconcile(property_id,month):
    property_scope(property_id)
    meters=[dict(r) for r in db.session.execute(text('SELECT id,meter_number,utility_type,parent_meter_id FROM bil_meter WHERE property_id=:p ORDER BY id'),dict(p=property_id)).mappings()]
    readings={}
    for row in db.session.execute(text('SELECT c.* FROM bil_consumption c JOIN bil_meter m ON m.id=c.meter_id WHERE m.property_id=:p AND c.month=:month ORDER BY c.id'),dict(p=property_id,month=month)).mappings():
        readings.setdefault(row['meter_id'],[]).append(row)
    result=[]
    for bulk in meters:
        children=[m for m in meters if m['parent_meter_id']==bulk['id']]
        if not children:continue
        group=[bulk]+children
        complete=all(len(readings.get(m['id'],[]))==1 for m in group)
        periods={(readings[m['id']][0].get('last_date'),readings[m['id']][0].get('new_date')) for m in group if len(readings.get(m['id'],[]))==1}
        complete=complete and len(periods)==1
        bulk_value=Decimal(str(readings[bulk['id']][0]['consumption'])) if complete else None
        individual=sum((Decimal(str(readings[m['id']][0]['consumption'])) for m in children),Decimal(0)) if complete else None
        difference=bulk_value-individual if complete else None
        result.append(dict(bulk=bulk,individual_count=len(children),bulk_consumption=bulk_value,individual_consumption=individual,difference=difference,chargeable_difference=max(Decimal(0),difference) if complete else None,allocation='Owner',status='Complete' if complete else 'Missing, duplicate or mismatched consumption periods — no chargeable difference confirmed'))
    return result
