"""HTML certificate schema and partial edits over the existing draft JSON keys."""
from copy import deepcopy
import json
from pathlib import Path
import re
from . import lease_certificate as certificate

LAYOUT = json.loads(Path(__file__).with_name('lease_certificate_layout.json').read_text(encoding='utf-8'))
TOKEN = re.compile(r'(\[[A-Z][A-Z0-9_]*\])')
MAX_PARTIES = 50


def counts(items):
    return (max(1, len(items.get('_contracting_lessors', {}).get('identities', [])) or 3),
            max(1, len(certificate.selected_tenants(items))))


def layout(lessors, lessees):
    sections = deepcopy(LAYOUT)
    rows = sections[0]['rows']
    owner = next(row for row in rows if '[LESSOR_1_NAME]' in row[1])
    tenant = [row for row in rows if '[LESSEE_1_' in row[1]]
    representative = [row for row in rows if '[LESSOR_SIGNATORY_' in row[1]]
    sections[0]['rows'] = [['Lease reference', '[LEASE_REFERENCE]']] + [
        ['Lessor '+str(i), owner[1].replace('LESSOR_1_', 'LESSOR_'+str(i)+'_')]
        for i in range(1, lessors+1)] + representative + [
        [label if i == 1 else label+' '+str(i), value.replace('LESSEE_1_', 'LESSEE_'+str(i)+'_')]
        for i in range(1, lessees+1) for label, value in tenant]
    return sections


def tokens(sections):
    return list(dict.fromkeys(token for section in sections for _, value in section['rows']
                              for token in TOKEN.findall(value)))


def readonly(token):
    return (
        token[1:-1].lower() in certificate.BANK_FIELDS or token.endswith('_AUTHORITY_CONFIRMED]'))


def render_rows(sections, values):
    seen = set()
    result = []
    for section in sections:
        rows = []
        for label, content in section['rows']:
            parts = []
            for part in TOKEN.split(content):
                if TOKEN.fullmatch(part):
                    parts.append({'token': part, 'name': part[1:-1], 'value': values.get(part, ''),
                                  'editable': not readonly(part) and part not in seen})
                    seen.add(part)
                elif part:
                    parts.append({'text': part})
            rows.append({'label': label, 'parts': parts})
        result.append({'title': section['title'], 'rows': rows})
    return result


def form_counts(form):
    try:
        lessors, lessees = (int(form[key]) for key in ('lessor_count', 'lessee_count'))
    except (KeyError, ValueError):
        raise ValueError('Reload the certificate before saving.') from None
    if not all(1 <= value <= MAX_PARTIES for value in (lessors, lessees)):
        raise ValueError('Use between 1 and 50 parties of each type.')
    return lessors, lessees


def submitted(form, sections):
    result = {}
    for token in tokens(sections):
        if readonly(token):
            continue
        name = token[1:-1]
        if name not in form:
            continue
        value = form[name].strip()
        if len(value) > 4000 or '\x00' in value:
            raise ValueError(name.replace('_', ' ').title()+' is too long or invalid.')
        if certificate.TOKEN.search(value):
            raise ValueError('Enter particulars or leave the field empty; do not save placeholder labels.')
        if value and (name.endswith('_DATE') or name in ('COMMENCEMENT_DATE','EXPIRY_DATE')):
            from datetime import date
            try: date.fromisoformat(value)
            except ValueError: raise ValueError(name.replace('_',' ').title()+' must use YYYY-MM-DD.') from None
        if value and (name in ('MONTHLY_RENT','DEPOSIT_AMOUNT','PARKING_RENT_PER_BAY') or re.fullmatch(r'ADJUSTMENT_[123]_AMOUNT',name)):
            from .lease_workflow import zar_amount
            zar_amount(value)
        result[token] = value
    return result


def merge(items, values, lessor_count, lessee_count):
    """Whitelist certificate fields; retain other records and unedited party metadata."""
    result = certificate.compatible_items(items)
    old_lessors = result.get('_contracting_lessors', {}).get('identities', [])
    lessors = [deepcopy(old_lessors[i]) if i < len(old_lessors) else {} for i in range(lessor_count)]
    old_tenants = certificate.selected_tenants(result)
    tenants = [deepcopy(old_tenants[i]) if i < len(old_tenants) else {} for i in range(lessee_count)]
    for token, value in values.items():
        name = token[1:-1]
        owner = re.fullmatch(r'LESSOR_(\d+)_(NAME|IDENTITY_NUMBER)', name)
        tenant = re.fullmatch(r'LESSEE_(\d+)_(LEGAL_NAME|IDENTITY_NUMBER|SIGNATORY_NAME|SIGNATORY_IDENTITY_NUMBER|SIGNATORY_CAPACITY|AUTHORITY_EVIDENCE)', name)
        if owner:
            lessors[int(owner[1])-1][owner[2].lower()] = value
        elif tenant:
            index = int(tenant[1])-1
            if tenant[2] == 'AUTHORITY_EVIDENCE':
                save_evidence(result, items, 'tenant-'+str(index), value)
            else: tenants[index][tenant[2].lower()] = value
        elif name.startswith('LESSOR_SIGNATORY_'):
            result.setdefault('_parties', {}).setdefault('lessor', {})[name[7:].lower()] = value
        elif name == 'LESSOR_AUTHORITY_EVIDENCE':
            save_evidence(result, items, 'landlords', value)
        elif name in certificate.LEGACY:
            result.setdefault(certificate.LEGACY[name], {})['value'] = value
        elif name.lower() in certificate.PREMISES:
            result.setdefault('_premises', {})[name.lower()] = value
        elif name in ('COMMENCEMENT_DATE','DURATION_MONTHS','EXPIRY_DATE','RENEWAL_NOTICE_DATE'):
            if name == 'DURATION_MONTHS' and value and (not value.isdigit() or int(value) > 1200):
                raise ValueError('Duration must be a whole number of months, up to 1200.')
            result.setdefault('_period', {})[name.lower()] = value
            for key, legacy in (('COMMENCEMENT_DATE','s1-18'),('DURATION_MONTHS','s1-17'),('EXPIRY_DATE','s1-20')):
                if name == key: result.setdefault(legacy, {})['value'] = value
        elif name.lower() in certificate.AMOUNTS:
            result.setdefault('_amounts', {})[name.lower()] = value
        elif name.startswith('UTILITY_'):
            result.setdefault('_banking_utilities', {})[name.lower()] = value
    for original, row in zip(old_lessors, lessors):
        if any(original.get(key,'') != row.get(key,'') for key in ('name','identity_number')):
            row.pop('owner_id',None)
    for original, row in zip(old_tenants, tenants):
        if any(original.get(key,'') != row.get(key,'') for key in ('legal_name','identity_number')):
            row.pop('tenant_id',None)
    if any(row for row in lessors) or '_contracting_lessors' in result:
        result.setdefault('_contracting_lessors', {})['identities'] = lessors
    if any(row for row in tenants) or '_parties' in result:
        result.setdefault('_parties', {})['lessees'] = tenants
        result['_parties']['lessee'] = deepcopy(tenants[0])
    utilities = result.get('_banking_utilities', {})
    if '[UTILITY_REFERENCES]' in values or '[UTILITY_NO_REFERENCES_REASON]' in values:
        refs = utilities.get('utility_references', '')
        reason = utilities.get('utility_no_references_reason', '')
        if refs and reason: raise ValueError('Supply utility references or a reason why none apply, not both.')
        utilities['utility_reference_mode'] = 'provided' if refs else 'none'
    # The existing fingerprint check invalidates authority after party edits.
    return result


def save_evidence(result, original, key, value):
    saved = original.get('_certificate_authorities', {}).get(key,
        original.get('_ait_standard', {}).get('authorities', {}).get(key, {}))
    if value != saved.get('evidence', ''):
        result.setdefault('_certificate_authorities', {})[key] = {'evidence': value, 'confirmed': False}


# Capture pages reuse the approved six-section field layout; only their owned keys may change.
CAPTURE_SECTIONS = (
    ('parties','1. Parties',0,('_contracting_lessors','_parties','_certificate_authorities','s1-01')),
    ('premises','2. Premises',1,('_premises',)),
    ('period','3. Lease Period',2,('_period','s1-18','s1-17','s1-20','s1-19','s1-26')),
    ('financial','4. Financial Particulars',3,('_amounts','_banking_utilities','s1-30','s1-31')),
    ('banking','5. Banking',4,('_certificate_banking',)),
    ('additional','6. Additional Particulars',5,('s1-25','s1-37','s1-34','s1-35','s1-29')),
)


def capture_section(key):
    return next((section for section in CAPTURE_SECTIONS if section[0] == key),None)


def merge_section(saved, displayed, edits, lessor_count, lessee_count, key):
    """Do not normalize or rewrite another section while saving a partial form."""
    section=capture_section(key)
    if section is None:raise ValueError('Choose a valid Data Capture section.')
    merged=merge(displayed,edits,lessor_count,lessee_count)
    result=deepcopy(saved)
    for name in section[3]:
        if name in merged:result[name]=deepcopy(merged[name])
    return result
