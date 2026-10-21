"""Supplementary conditions in draft JSON; legacy particulars are never rewritten."""
from copy import deepcopy

KEY = '_supplementary_conditions'
HEADINGS = ('Deposit Refund \u2014 Lessee Banking Details', 'Rental Adjustments', 'Parking / Other Charges', 'Utilities / Allocation', 'Custom')
MAX_CLAUSES = 50
MAX_TEXT = 20000


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
            for index in range(1, min(MAX_CLAUSES, max(0, count)) + 1)]
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
    if not 0 <= count <= MAX_CLAUSES or (mode == 'add' and count == 0):
        raise ValueError('Use between 1 and 50 supplementary clauses, or choose None.')
    clauses = []
    for index in range(1, count + 1):
        kind = posted.get('sc_heading_type_' + str(index), '').strip()
        custom = posted.get('sc_custom_heading_' + str(index), '').strip()
        body = posted.get('sc_text_' + str(index), '').strip()
        if kind not in HEADINGS or len(custom) > 200 or len(body) > MAX_TEXT or '\x00' in custom + body:
            raise ValueError('SC-%02d: enter a valid heading and clause text.' % index)
        heading = custom if kind == 'Custom' else kind
        if mode == 'add' and (not heading or not body):
            raise ValueError('SC-%02d: supply a clause heading and clause text, or remove the clause.' % index)
        clauses.append({'heading_type': kind, 'heading': heading, 'text': body})
    if sum(len(row['text']) + len(row['heading']) for row in clauses) > MAX_TEXT:
        raise ValueError('Keep supplementary conditions within 20000 characters in total.')
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
