"""Supplementary conditions in draft JSON; legacy particulars are never rewritten."""
from copy import deepcopy

KEY = '_supplementary_conditions'
HEADINGS = ('Deposit Refund \u2014 Lessee Banking Details', 'Rental Adjustments', 'Parking / Other Charges', 'Utilities / Allocation', 'Custom')


def editor(items, posted=None):
    saved = items.get(KEY, {})
    result = deepcopy(saved) if isinstance(saved, dict) else {}
    result.setdefault('mode', '')
    if not isinstance(result.get('clauses'),list):result['clauses']=[]
    if posted is not None:
        result['mode'] = posted.get('sc_mode', '')
        try: count = int(posted.get('sc_count', '0'))
        except ValueError: count = 0
        result['clauses'] = [
            {'heading_type': posted.get('sc_heading_type_' + str(index), ''),
             'heading': posted.get('sc_custom_heading_' + str(index), ''),
             'text': posted.get('sc_text_' + str(index), '')}
            for index in range(1, min(len(posted) // 3, max(0, count)) + 1)]
    return result


def submitted(posted, items):
    if posted.get('sc_editor_version') != '1':
        raise ValueError('Reload the Supplementary Conditions editor before saving.')
    mode = posted.get('sc_mode', '')
    if mode not in ('', 'none', 'add'):
        raise ValueError('Choose None or Add SC.')
    if not mode:
        return deepcopy(items.get(KEY))
    try: count = int(posted.get('sc_count', '0'))
    except ValueError: raise ValueError('Reload the Supplementary Conditions editor.') from None
    # Bound iteration by supplied fields, not an arbitrary clause limit.
    if count < 0 or count > len(posted) // 3 or (mode == 'add' and count == 0):
        raise ValueError('Supply complete supplementary clauses, or choose None.')
    clauses = []
    for index in range(1, count + 1):
        kind = posted.get('sc_heading_type_' + str(index), '').strip()
        custom = posted.get('sc_custom_heading_' + str(index), '').strip()
        body = posted.get('sc_text_' + str(index), '').strip()
        if kind not in HEADINGS or len(custom) > 200 or '\x00' in custom + body:
            raise ValueError('SC-%02d: enter a valid heading and clause text.' % index)
        heading = custom if kind == 'Custom' else kind
        if mode == 'add' and (not heading or not body):
            raise ValueError('SC-%02d: supply a clause heading and clause text, or remove the clause.' % index)
        clauses.append({'heading_type': kind, 'heading': heading, 'text': body})
    # None omits output while retaining clauses so toggling does not discard saved work.
    return {'schema_version': 1, 'mode': mode, 'clauses': clauses}


def preview(items):
    saved = items.get(KEY)
    if not isinstance(saved, dict) or saved.get('mode') not in ('none', 'add'):
        return None
    if saved['mode'] == 'none':
        return {'available': True, 'printable': False, 'state': 'none', 'omitted': True,
                'clauses': [], 'reason': 'No supplementary conditions are required.'}
    clauses = deepcopy(saved.get('clauses', []))
    complete = bool(clauses) and all(isinstance(row,dict) and row.get('heading') and row.get('text') for row in clauses)
    return {'available': complete, 'printable': complete, 'state': 'draft', 'clauses': clauses,
            'reason': 'Complete the supplementary clause headings and text before previewing.'}


def legacy_entries(items):
    """Read-only inventory; never guess whether old terms were superseded by the editor."""
    import json,re
    entries=[]
    def add(key,value):
        if value is None or value == '' or value == [] or value == {}:return
        text=value if isinstance(value,str) else json.dumps(value,ensure_ascii=False,sort_keys=True,default=str)
        if text.strip():entries.append({'source':key,'text':text})
    for key in sorted(items):
        if re.fullmatch(r'sc-\d+',key):
            saved=items[key]
            add(key,saved.get('value') if isinstance(saved,dict) and set(saved)<= {'value'} else saved)
    add('_additional_clauses',items.get('_additional_clauses'))
    for key in ('rental_adjustments_text','parking_other_charges_text','utilities_allocation_text'):
        add('_amounts.'+key,items.get('_amounts',{}).get(key))
    return entries
