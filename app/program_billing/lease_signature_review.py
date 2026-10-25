"""Content-bound final user review; never an approval or signature substitute."""
from hashlib import sha256
import json
from datetime import datetime,timezone

ACKNOWLEDGEMENT='I confirm that I have reviewed the complete Lease Agreement and am satisfied that it is ready for signature.'


def fingerprint(items):
    from .lease_kit_pdf import CORE_SHA256
    body={k:v for k,v in items.items() if k not in ('_signature_review','_signature_review_history','_ait_standard','_generations','_lease_history')}
    return sha256(json.dumps({'items':body,'core':CORE_SHA256},sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def requires_pdf(items):
    saved=items.get('_supplementary_conditions',{})
    return bool((saved.get('mode')=='add' and saved.get('clauses')) or items.get('_map_annexures') or items.get('_additional_clauses') or any(key.startswith('sc-') and value for key,value in items.items()) or any(items.get('_amounts',{}).get(key) for key in ('rental_adjustments_text','parking_other_charges_text','utilities_allocation_text')))


def valid(items):
    if requires_pdf(items) and not items.get('_sc_pdf_current'):return False
    saved=items.get('_signature_review',{})
    return saved.get('confirmed') is True and saved.get('fingerprint')==fingerprint(items)


def record(items,user_id):
    review={'confirmed':True,'fingerprint':fingerprint(items),'by':int(user_id),'at':datetime.now(timezone.utc).isoformat(),'acknowledgement':ACKNOWLEDGEMENT}

    items.setdefault('_signature_review_history',[]).append(review)
    items['_signature_review']=review
