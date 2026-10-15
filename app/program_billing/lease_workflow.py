"""Five Lease workspaces over the original R01 field keys, not a new lease model."""
import calendar
import hashlib
import json
import re
from pathlib import Path
from datetime import date, timedelta
from flask import abort, flash, redirect, request, url_for
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing.lease_management import render_pm_template

# R01 contains 41 unnumbered Schedule 1 rows. These are the original catalog keys,
# grouped semantically; the requested 1-25 numbering must not replace source keys.
GROUPS = {
    'parties': ('A. Parties', [1, 2, 3, 4, 5, 6, 7, 8, 9, 39, 41]),
    'premises': ('B. Premises', [10, 11, 12, 13, 14, 15, 16]),
    'period': ('C. Period', [17, 18, 19, 20]),
    'amounts': ('D. Amounts', list(range(21, 39))),
    'conditions': ('E. Special Conditions', [40]),
}
GROUP_KEYS = {g: [f's1-{n:02d}' for n in numbers] for g, (_, numbers) in GROUPS.items()}
GROUP_KEYS['conditions'] += [f'sc-{n:02d}' for n in range(1, 11)]
KEY_GROUP = {key: group for group, keys in GROUP_KEYS.items() for key in keys}
MASTER_BLOCKER = ('Reusable master uploads and generation are not enabled. An approved, versioned master registry '
                  'and immutable generation history are not present in the proposed two-table schema. '
                  'Schema approval, private R2 verification and an approved master are required.')
MANIFEST = json.loads(Path(__file__).with_name('lease_reference_manifest.json').read_text(encoding='utf-8'))

if MANIFEST['source_sha256'] != snapshot.CATALOG['source_docx_sha256']:
    raise RuntimeError('R01 reference manifest/source mismatch')


def selected_context(property_id, lock=False):
    prop = snapshot.authorised_property(property_id, lock)
    reason = snapshot.readiness()
    draft = None
    if not reason and request.values.get('draft_id'):
        draft = snapshot.draft_row(property_id, request.values['draft_id'], lock)
    return prop, reason, draft


def save_items(draft, items):
    try: version = int(request.form.get('version', ''))
    except ValueError: abort(400, description='Reload the draft before saving.')
    updated = db.session.execute(text('''UPDATE bil_lease_draft
        SET items=CAST(:items AS jsonb),version=version+1,updated_at=now()
        WHERE id=:d AND property_id=:p AND manager_id=:u AND version=:v RETURNING id'''),
        {'items':json.dumps(items), 'd':draft['id'], 'p':draft['property_id'],
         'u':current_user.id, 'v':version}).scalar_one_or_none()
    if updated is None: abort(409, description='The draft changed. Reload before saving.')
    db.session.commit()


def group_url(property_id, group, draft=None):
    return url_for('billing_bp.lease_particular_group', property_id=property_id,
                   group=group, draft_id=draft['id'] if draft else None)


def period_alerts(items):
    def value(key): return items.get(key, {}).get('value', '')
    alerts = []
    start, end = value('s1-18'), value('s1-20')
    try:
        start = date.fromisoformat(start) if start else None
        end = date.fromisoformat(end) if end else None
        occupation = value('s1-19')
        if occupation: date.fromisoformat(occupation)
    except ValueError:
        return ['C. Period: enter valid ISO lease dates.']
    if start and end and end < start: alerts.append('C. Period: expiry precedes commencement.')
    term = value('s1-17')
    if term:
        match = re.fullmatch(r'\s*(\d+)\s*(months?|years?)\s*', term, re.I)
        if not match:
            alerts.append('C. Period: duration must be explicit, for example 12 months or 2 years, for consistency checking.')
        else:
            months = int(match[1]) * (12 if match[2].lower().startswith('year') else 1)
            if not 1 <= months <= 1200:
                alerts.append('C. Period: duration must be between 1 and 1200 months.')
            elif start and end:
                year, month = divmod(start.year * 12 + start.month - 1 + months, 12)
                if year > 9999:
                    alerts.append('C. Period: duration exceeds the supported calendar range.')
                else:
                    expected = date(year, month + 1, min(start.day, calendar.monthrange(year, month + 1)[1])) - timedelta(days=1)
                    if end != expected:
                        alerts.append(f'C. Period: {term} from {start} has inclusive expiry {expected}; entered expiry is {end}.')
    return alerts


def reference_alerts(items):
    """Conservative lexical checks against the verified R01, never silent renumbering."""
    alerts = []
    known = set(MANIFEST['clauses'])
    additional = items.get('_additional_clauses', [])
    known.update(str(c['number']) for c in additional)
    condition_entries = [(key, items.get(key, {})) for key in GROUP_KEYS['conditions']]
    condition_entries += [('additional-clauses', {'value':c['text']}) for c in additional]
    sc_ids = set(f'SC-{n:02d}' for n in range(1, 11))
    for key, entry in condition_entries:
        if entry.get('not_applicable'): continue
        value = entry.get('value', '')
        for number in re.findall(r'(?i)\b(?:core|clause)\s+(\d+(?:\.\d+)*)', value):
            if number not in known:
                alerts.append((key, f'Unknown master clause reference {number}.'))
        for number in re.findall(r'(?i)\bSC-(\d+)\b', value):
            identifier = 'SC-' + number.zfill(2)
            if identifier not in sc_ids: alerts.append((key, f'Unknown special-condition reference {identifier}.'))
            elif not items.get(identifier.lower(), {}).get('value'):
                alerts.append((key, f'Reference {identifier} has no recorded condition text.'))
        for number in re.findall(r'(?i)\bSchedule\s+(\d+)', value):
            if number not in ('1', '2'): alerts.append((key, f'Unknown Schedule {number}.'))
        for letter in re.findall(r'(?i)\bAnnexure\s+([A-Z])\b', value):
            target = 'annexure-' + letter.lower()
            if target not in snapshot.ITEMS: alerts.append((key, f'Unknown Annexure {letter.upper()}.'))
            elif not items.get(target, {}).get('value'):
                alerts.append((key, f'Annexure {letter.upper()} is referenced without recorded particulars.'))
        if re.search(r'(?i)\b(?:above|below|preceding|following|same)\s+(?:clause|schedule|annexure)|\b(?:clause|schedule|annexure)\s+(?:above|below|\?|TBC|XX)\b', value):
            alerts.append((key, 'Ambiguous cross-reference: use an explicit verified identifier.'))
        numbered = re.findall(r'(?m)^\s*(\d+(?:\.\d+)*)[.)]?\s+\S', value)
        if numbered:
            alerts.append((key, 'Embedded clause numbering requires review; use the existing SC identifier. No automatic renumbering was performed.'))
    for index, clause in enumerate(additional, MANIFEST['last_core_clause'] + 1):
        if clause['number'] != index:
            alerts.append(('additional-clauses', f'Additional clause {clause["number"]}: expected consecutive identifier {index}; numbering was not changed.'))
    return alerts


def completion_alerts(items, files):
    alerts = []
    for group, keys in GROUP_KEYS.items():
        for key in keys:
            field = snapshot.ITEMS[key]; data = items.get(key, {})
            if data.get('not_applicable'):
                if field['requirement'] != 'Conditional' or len(data.get('reason', '')) < 10:
                    alerts.append((group, key, f"{field['label']}: invalid Not Applicable declaration."))
            elif field['requirement'] in ('Essential', 'Conditional') and not data.get('value', '').strip():
                alerts.append((group, key, f"{field['label']}: {field['requirement']} information or applicability is missing."))
            elif any(token in data.get('value', '') for token in field.get('placeholders', [])):
                alerts.append((group, key, f"{field['label']}: replace the remaining merge placeholder."))
    if not items.get('_contracting_lessors', {}).get('confirmed'):
        alerts.append(('parties', 's1-03', 'Confirm and snapshot the actual contracting lessor identities. Registered ownership alone is not confirmation.'))
    for message in period_alerts(items): alerts.append(('period', 's1-17', message))
    for key, message in reference_alerts(items): alerts.append(('conditions', key, message))
    requirements = items.get('_annexure_requirements', {})
    referenced = set()
    for key in GROUP_KEYS['conditions']:
        if not items.get(key, {}).get('not_applicable'):
            referenced.update('annexure-' + letter.lower() for letter in re.findall(r'(?i)\bAnnexure\s+([A-Z])\b', items.get(key, {}).get('value', '')))
    for key, field in snapshot.ITEMS.items():
        if not key.startswith('annexure-'): continue
        classification = requirements.get(key, field['requirement'])
        data = items.get(key, {})
        if classification not in ('Essential', 'Conditional', 'Optional'):
            alerts.append(('supporting', key, f'{field["label"]}: invalid requirement classification.')); continue
        na = data.get('not_applicable')
        if na and (classification != 'Conditional' or len(data.get('reason', '')) < 10):
            alerts.append(('supporting', key, f'{field["label"]}: Not Applicable requires conditional classification and an explanation.'))
        required = classification == 'Essential' or (classification == 'Conditional' and not na) or key in referenced
        if required and not any(f['item_key'] == key and f['state'] == 'available' for f in files):
            alerts.append(('supporting', key, f'{field["label"]}: required or referenced supporting document is missing.'))
    for clause in items.get('_additional_clauses', []):
        for letter in re.findall(r'(?i)\bAnnexure\s+([A-Z])\b', clause['text']):
            key = 'annexure-' + letter.lower()
            if not any(f['item_key'] == key and f['state'] == 'available' for f in files):
                alerts.append(('supporting', key, f'Additional clause {clause["number"]}: referenced Annexure {letter.upper()} is missing.'))
    return alerts


def owners_for(property_id):
    from app.program_billing.owner_management import owners_enabled, owner_rows
    return owner_rows(property_id) if owners_enabled() else []


@billing_bp.get('/billing/leases/property/<int:property_id>/particulars', endpoint='lease_particulars')
@snapshot.endpoint
def particulars(property_id):
    prop, reason, draft = selected_context(property_id)
    items = draft['items'] if draft else {}
    groups = []
    for key, (title, _) in GROUPS.items():
        fields = [snapshot.ITEMS[k] for k in GROUP_KEYS[key]]
        recorded = sum(bool(items.get(f['key'], {}).get('value') or items.get(f['key'], {}).get('not_applicable')) for f in fields)
        groups.append({'title':title, 'url':group_url(property_id, key, draft),
                       'summary':f'{recorded} of {len(fields)} particulars recorded; not legal approval'})
    return render_pm_template('program_billing/lease_particulars.html', property=prop, lease_draft=draft,
                              unavailable=reason, groups=groups)


@billing_bp.route('/billing/leases/property/<int:property_id>/particulars/<group>', methods=['GET', 'POST'], endpoint='lease_particular_group')
@snapshot.endpoint
def particular_group(property_id, group):
    if group not in GROUPS: abort(404)
    prop, reason, draft = selected_context(property_id, request.method == 'POST')
    items = dict(draft['items']) if draft else {}
    fields = [snapshot.ITEMS[k] for k in GROUP_KEYS[group]]
    if group == 'premises':
        items.setdefault('s1-10', {'value':prop['name']})
        items.setdefault('s1-11', {'value':prop['address'] or ''})
    owners = owners_for(property_id) if group == 'parties' else []
    saved_lessors = items.get('_contracting_lessors', {}).get('identities', [])
    candidates = {str(o['owner_id']):dict(o) for o in owners}
    # Saved contracting identities remain available even if the current register changes.
    for owner in saved_lessors: candidates[str(owner['owner_id'])] = owner
    selected_ids = [str(o['owner_id']) for o in saved_lessors]
    errors = []
    additional_text = '\n'.join(str(c['number']) + ' | ' + c['text'] for c in items.get('_additional_clauses', []))
    if request.method == 'POST':
        snapshot.require_schema()
        if not draft: abort(400, description='Start and select a persistent draft first.')
        for field in fields:
            key = field['key']
            value = request.form.get(key, '').strip()
            na = request.form.get(key + '_na') == 'on'
            explanation = request.form.get(key + '_reason', '').strip()
            items[key] = {'value':value, 'not_applicable':na, 'reason':explanation}
            if len(value) > 4000 or len(explanation) > 500: errors.append(field['label'] + ': text is too long.')
            if na and (field['requirement'] != 'Conditional' or len(explanation) < 10):
                errors.append(field['label'] + ': Not Applicable is conditional only and needs an explanation.')
            if field.get('input_type') == 'date' and value and not na:
                try: date.fromisoformat(value)
                except ValueError: errors.append(field['label'] + ': enter a valid date.')
            if any(token in value for token in field.get('placeholders', [])):
                errors.append(field['label'] + ': replace the R01 placeholder with captured details.')
        if group == 'parties':
            selected_ids = request.form.getlist('lessor_id')
            if not selected_ids or request.form.get('confirm_lessors') != 'on':
                errors.append('Select and confirm the actual contracting lessor(s); property ownership does not establish contracting capacity.')
            elif len(set(selected_ids)) != len(selected_ids) or any(identifier not in candidates for identifier in selected_ids):
                abort(400, description='Choose authorized registered or already snapshotted lessors.')
            else:
                identities = [{key:owner.get(key) for key in ('owner_id', 'type', 'name', 'identity_number', 'email', 'phone', 'residential_address', 'postal_address')}
                              for owner in (candidates[identifier] for identifier in selected_ids)]
                # Never re-read current owner details to silently change a saved snapshot.
                items['_contracting_lessors'] = {'confirmed':True, 'identities':identities}
                items['s1-03'] = {'value':'\n'.join(o['name'] for o in identities)}
                if not items['s1-05']['value']:items['s1-05'] = {'value':'\n\n'.join('\n'.join(filter(None, (o['name'], o.get('residential_address'), o.get('email'), o.get('phone')))) for o in identities)}
        if group == 'conditions':
            additional_text = request.form.get('additional_clauses', '').strip()
            additional = []
            if len(additional_text) > 20000: errors.append('Keep additional clauses within 20000 characters.')
            for index, line in enumerate(additional_text.splitlines(), MANIFEST['last_core_clause'] + 1):
                match = re.fullmatch(r'\s*(\d+)\s*\|\s*(.+)', line)
                if not match or int(match[1]) != index:
                    errors.append(f'Additional clauses must continue the R01 core sequence: expected {index} | text. No renumbering was performed.')
                    continue
                content = match[2].strip()
                digest = hashlib.sha256(' '.join(content.lower().split()).encode()).hexdigest()
                if digest in MANIFEST['clause_text_hashes']: errors.append(f'Clause {index} duplicates existing master text; enter only additional conditions.')
                additional.append({'number':index, 'text':content})
            items['_additional_clauses'] = additional
        if group == 'period': errors += period_alerts(items)
        if not errors:
            save_items(draft, items)
            flash('Lease particulars saved. This does not approve or activate the lease.', 'success')
            return redirect(url_for('billing_bp.lease_particulars', property_id=property_id, draft_id=draft['id']))
    return render_pm_template('program_billing/lease_particular_group.html', property=prop, lease_draft=draft,
        unavailable=reason, group=group, title=GROUPS[group][0], fields=fields, items=items, errors=errors,
        lessors=list(candidates.values()), selected_lessor_ids=selected_ids, additional_text=additional_text, next_clause=MANIFEST['last_core_clause']+1), (400 if errors else 200)


@billing_bp.get('/billing/leases/property/<int:property_id>/master', endpoint='lease_master')
@snapshot.endpoint
def master(property_id):
    prop, reason, draft = selected_context(property_id)
    is_admin = bool(getattr(current_user, 'has_role', lambda role:False)('admin'))
    from app.program_billing import lease_storage
    try: bucket = lease_storage.destination(); storage_reason = None
    except ValueError as error: bucket = None; storage_reason = str(error)
    return render_pm_template('program_billing/lease_master.html', property=prop, lease_draft=draft,
        unavailable=reason, is_admin=is_admin, blocker=MASTER_BLOCKER, storage_reason=storage_reason)


@billing_bp.route('/billing/leases/property/<int:property_id>/supporting', methods=['GET', 'POST'], endpoint='lease_supporting')
@snapshot.endpoint
def supporting(property_id):
    prop, reason, draft = selected_context(property_id, request.method == 'POST')
    items = dict(draft['items']) if draft else {}
    annexures = [i for i in snapshot.CATALOG['items'] if i['key'].startswith('annexure-')]
    errors = []
    if request.method == 'POST':
        snapshot.require_schema()
        if not draft: abort(400, description='Select a persistent draft first.')
        requirements = {}
        for field in annexures:
            key = field['key']; classification = request.form.get(key + '_requirement', field['requirement'])
            value = request.form.get(key, '').strip(); na = request.form.get(key + '_na') == 'on'
            explanation = request.form.get(key + '_reason', '').strip()
            if classification not in ('Essential', 'Conditional', 'Optional'): errors.append('Choose a valid annexure classification.')
            if len(value) > 4000 or len(explanation) > 500: errors.append(field['label'] + ': text is too long.')
            if na and (classification != 'Conditional' or len(explanation) < 10): errors.append(field['label'] + ': Not Applicable requires conditional classification and an explanation.')
            requirements[key] = classification; items[key] = {'value':value, 'not_applicable':na, 'reason':explanation}
        items['_annexure_requirements'] = requirements
        if not errors:
            save_items(draft, items); flash('Supporting requirements saved; attachments remain subject to review.', 'success')
            return redirect(url_for('billing_bp.lease_supporting', property_id=property_id, draft_id=draft['id']))
    return render_pm_template('program_billing/lease_supporting.html', property=prop, lease_draft=draft,
        unavailable=reason, annexures=annexures, items=items, errors=errors, files=snapshot.documents(draft)), (400 if errors else 200)


@billing_bp.get('/billing/leases/property/<int:property_id>/review', endpoint='lease_review')
@snapshot.endpoint
def review(property_id):
    prop, reason, draft = selected_context(property_id)
    issues = completion_alerts(draft['items'] if draft else {}, snapshot.documents(draft))
    alerts = []
    for group, key, message in issues:
        target = (url_for('billing_bp.lease_supporting', property_id=property_id, draft_id=draft['id'] if draft else None)
                  if group == 'supporting' else group_url(property_id, group, draft))
        alerts.append({'message':message, 'url':target + '#' + key})
    return render_pm_template('program_billing/lease_review.html', property=prop, lease_draft=draft,
        unavailable=reason, alerts=alerts, blocker=MASTER_BLOCKER)


@billing_bp.after_request
def protect_workflow(response):
    if request.endpoint in {'billing_bp.lease_particulars', 'billing_bp.lease_particular_group',
            'billing_bp.lease_master', 'billing_bp.lease_supporting', 'billing_bp.lease_review'}:
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
    return response
