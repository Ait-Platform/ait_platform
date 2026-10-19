"""Five Lease workspaces over the original R01 field keys, not a new lease model."""
import calendar
from copy import deepcopy
import hashlib
import json
import re
import uuid
from pathlib import Path
from io import BytesIO
from datetime import date, timedelta, datetime, timezone
from decimal import Decimal, InvalidOperation
from flask import abort, flash, redirect, request, url_for, g, send_file
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing import tenant_register
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
    reason = snapshot.particulars_readiness()
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


def create_parties_draft(property_id, items):
    """First valid Part A save: one insert/commit with existing token uniqueness."""
    try:token=uuid.UUID(request.form.get('registration_token',''))
    except (ValueError,TypeError):abort(400,description='Reload Part A before starting a draft.')
    draft=db.session.execute(text("""INSERT INTO bil_lease_draft
        (id,property_id,manager_id,registration_token,source_version,items)
        VALUES (:id,:p,:u,:token,:source,CAST(:items AS jsonb))
        ON CONFLICT (manager_id,registration_token) DO NOTHING RETURNING *"""),
        {'id':uuid.uuid4(),'p':property_id,'u':current_user.id,'token':token,
         'source':snapshot.CATALOG['version'],'items':json.dumps(items)}).mappings().first()
    if draft is None:
        existing_id=db.session.execute(text('SELECT id FROM bil_lease_draft WHERE manager_id=:u AND registration_token=:t AND property_id=:p'),
            {'u':current_user.id,'t':token,'p':property_id}).scalar_one_or_none()
        if existing_id is None:abort(409,description='This registration token belongs to another draft context. Reload Part A.')
        draft=snapshot.draft_row(property_id,existing_id,True)
        if draft['items']!=items:
            abort(409,description='This draft was already saved. Reopen it before making further changes.')
    db.session.commit()
    return draft


def group_url(property_id, group, draft=None):
    return url_for('billing_bp.lease_particular_group', property_id=property_id,
                   group=group, draft_id=draft['id'] if draft else None)


def period_alerts(items, require_complete=False):
    def value(key): return items.get(key, {}).get('value', '').strip()
    alerts = []
    dates = {}
    for key, label in (('s1-18','commencement'),('s1-19','occupation'),('s1-20','expiry')):
        raw = value(key)
        if not raw:
            dates[key] = None
            if require_complete:alerts.append('C. Period: ' + label + ' date is required.')
            continue
        try:
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',raw):raise ValueError()
            dates[key] = date.fromisoformat(raw)
        except ValueError:
            alerts.append('C. Period: ' + label + ' must be a valid YYYY-MM-DD date.')
            dates[key] = None
    start, occupation, end = (dates[key] for key in ('s1-18','s1-19','s1-20'))
    if start and end and end < start:alerts.append('C. Period: expiry precedes commencement.')
    if occupation and end and occupation > end:alerts.append('C. Period: occupation falls after lease expiry.')
    if occupation and start and occupation < start:
        alerts.append('C. Period: occupation precedes commencement. Review the dates and any separately agreed early-occupation terms; no such terms have been assumed.')
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
        if group == 'amounts' and items.get('_amounts'):
            form = {'amounts_form_version':str(items.get('_amounts',{}).get('schema_version',1)), **{'amounts_' + key:value for key,value in amounts_state(items).items()}}
            _, financial_errors = amounts_from_form(form, items)
            alerts.extend(('amounts','s1-21',message) for message in financial_errors)
            # The Stage 5 master has no annual-rate/fee placeholders. Retain the
            # remaining ancillary review checks without changing their saved data.
            keys = ['s1-36','s1-37','s1-38']
        for key in keys:
            field = period_field(snapshot.ITEMS[key]) if group == 'period' else snapshot.ITEMS[key]
            data = items.get(key, {})
            if data.get('not_applicable'):
                if field['requirement'] != 'Conditional' or len(data.get('reason', '')) < 10:
                    alerts.append((group, key, f"{field['label']}: invalid Not Applicable declaration."))
            elif field['requirement'] in ('Essential', 'Conditional') and not data.get('value', '').strip():
                alerts.append((group, key, f"{field['label']}: {field['requirement']} information or applicability is missing."))
            elif any(token in data.get('value', '') for token in field.get('placeholders', [])):
                alerts.append((group, key, f"{field['label']}: replace the remaining merge placeholder."))
    if items.get('_banking_utilities'):
        ancillary_form = {'part5_form_version':'1', **{'part5_' + key:value for key,value in banking_utilities_state(items).items() if isinstance(value,str)}}
        for key in ('utilities_confirmed','banking_confirmed'):
            ancillary_form['part5_' + key] = 'on' if items['_banking_utilities'].get(key) is True else ''
        _, ancillary_errors = banking_utilities_from_form(ancillary_form)
        alerts.extend(('amounts','banking-utilities',message) for message in ancillary_errors)
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
    from app.program_billing.owner_management import owner_reads_available, owner_rows
    return owner_rows(property_id) if owner_reads_available() else []


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
    fields = [period_field(snapshot.ITEMS[k]) if group == 'period' else snapshot.ITEMS[k] for k in GROUP_KEYS[group]]
    if group == 'premises':
        items.setdefault('s1-10', {'value':prop['name']})
        items.setdefault('s1-11', {'value':prop['address'] or ''})
    owners = owners_for(property_id) if group == 'parties' else []
    saved_lessors = items.get('_contracting_lessors', {}).get('identities', [])
    candidates = {str(o['owner_id']):dict(o) for o in owners}
    # Saved contracting identities remain available even if the current register changes.
    for owner in saved_lessors: candidates[str(owner['owner_id'])] = owner
    selected_ids = [str(o['owner_id']) for o in saved_lessors]
    party_data = parties_state(items) if group == 'parties' else {}
    premises_data = premises_state(items, prop) if group == 'premises' else {}
    amounts_data = amounts_state(items) if group == 'amounts' else {}
    ancillary_data = banking_utilities_state(items) if group == 'amounts' else {}
    ancillary_submission = group == 'amounts' and request.form.get('workspace_section') == 'banking_utilities'
    errors = []
    additional_text = '\n'.join(str(c['number']) + ' | ' + c['text'] for c in items.get('_additional_clauses', []))
    if request.method == 'POST':
        snapshot.require_particulars_schema()
        if not draft and group!='parties': abort(400, description='Start and select a persistent draft first.')
        for field in fields:
            key = field['key']
            if group == 'period' and request.form.get('period_form_version')=='2':continue
            if group == 'amounts':continue  # Structured capture preserves other legacy particulars.
            if group == 'parties' and (request.form.get('parties_form_version') in ('2','3','4','5','6') or key in STRUCTURED_PARTY_KEYS):continue
            if group == 'premises':continue  # Hidden legacy particulars must not be cleared on save.
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
            registry_mode = request.form.get('parties_form_version') in ('3','4','5','6')
            if request.form.get('parties_form_version')!='6':
                abort(400,description='Reload Part A. Change tenants only on Add / Select Tenants.')
            tenant_snapshots=selected_party_snapshots(items)
            if not tenant_snapshots:abort(400,description='Use Add / Select Tenants to save at least one contracting tenant first.')
            if tenant_snapshot_errors(items):abort(400,description='Review the saved tenant particulars on Add / Select Tenants.')
            party_form=request.form.copy()
            party_form['parties_form_version']='3'
            first=tenant_snapshots[0]
            for key in tenant_register.LEASE_FIELDS:party_form['lessee_'+key]=first.get(key,'') or ''
            party_data,party_errors=parties_from_form(party_form,items,owners,first)
            # Selection belongs exclusively to the tenant page. Preserve the exact
            # saved identities/representatives even if registry records later change.
            party_data['lessee']=deepcopy(first)
            party_data['lessees']=deepcopy(tenant_snapshots)
            errors.extend(party_errors)
            selected_ids = [str(o['owner_id']) for o in owners] if registry_mode else request.form.getlist('lessor_id')
            if registry_mode:candidates = {str(o['owner_id']):dict(o) for o in owners}
            if not selected_ids or (request.form.get('parties_form_version') not in ('2','3','4','5','6') and request.form.get('confirm_lessors') != 'on'):
                errors.append('Add at least one property owner as a lessor.' if request.form.get('parties_form_version') in ('2','3','4','5','6') else 'Select and confirm the actual contracting lessor(s); property ownership does not establish contracting capacity.')
            elif len(set(selected_ids)) != len(selected_ids) or any(identifier not in candidates for identifier in selected_ids):
                abort(400, description='Choose authorized registered or already snapshotted lessors.')
            elif not errors:
                identities = [{key:owner.get(key) for key in ('owner_id', 'type', 'name', 'identity_number', 'email', 'phone', 'residential_address', 'postal_address')}
                              for owner in (candidates[identifier] for identifier in selected_ids)]
                if request.form.get('parties_form_version') in ('2','3','4','5','6') and any(not (owner.get('identity_number') or '').strip() for owner in identities):
                    errors.append('Every selected owner requires an ID or registration number in the property owner register.')
                if not errors:
                    if registry_mode and record_representative_change(items, party_data):
                        save_items(draft, items)
                        flash('Representative change recorded for addendum and Lessee-notice review. The original representative was preserved; no notice or addendum was issued.', 'warning')
                        return redirect(group_url(property_id, 'parties', draft))
                    if items.get('_parties', {}).get('schema_version') in (1,2):
                        items.setdefault('_legacy_parties', deepcopy(items['_parties']))
                        if registry_mode and items.get('_contracting_lessors'):
                            items.setdefault('_legacy_contracting_lessors', deepcopy(items['_contracting_lessors']))
                    items['_contracting_lessors'] = {'confirmed':True, 'identities':identities}
                    items['_parties'] = party_data
                    values = party_placeholder_values(items)
                    # Retain established R01 keys for existing summaries and completion checks.
                    for key, value in {
                        's1-03':values['[S1 LEGAL LANDLORD(S)]'],
                        's1-04':values['[LANDLORD SIGNATORY NAME AND CAPACITY]'],
                        's1-05':values['[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]'],
                        's1-06':values['[S1 LEGAL TENANT]'],
                        's1-07':values['[TENANT REGISTRATION OR ID]'],
                        's1-08':values['[TENANT SIGNATORY NAME AND CAPACITY]'],
                        's1-09':values['[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]'],
                        's1-41':'\n'.join(token + ': ' + values[token] for token in SIGNATURE_TOKENS if values[token]),
                    }.items():
                        items[key] = {'value':value}
        if group == 'premises':
            premises_data, premises_errors = premises_from_form(request.form, items.get('_premises',{}))
            errors.extend(premises_errors)
            if not errors:
                items['_premises'] = premises_data
                if request.form.get('premises_form_version')=='2':
                    # Only the visible name/address change. Keep all historic
                    # premises values and R01 summary keys exactly as saved.
                    items['s1-10']={'value':premises_data['property_name']}
                    items['s1-11']={'value':premises_data['property_address']}
                else:
                    # Legacy structured submissions retain their existing mappings.
                    for key, value in {
                        's1-10':premises_data['property_name'],
                        's1-11':premises_data['property_address'],
                        's1-12':premises_data['leased_portion_description'] + '\nUnits / portions: ' + premises_data['unit_count'],
                        's1-13':'Whole-site extent (m?): ' + premises_data['property_extent_sqm'] + '\nLeased area (m?): ' + premises_data['leased_area_sqm'] + '\nExcluded areas: ' + premises_data['excluded_areas'],
                        's1-14':'Plan A: ' + premises_data['plan_a_identifier'] + '\nPlan B: ' + premises_data['plan_b_identifier'],
                    }.items():items[key] = {'value':value}
        if group == 'amounts' and not ancillary_submission:
            amounts_data, financial_errors = amounts_from_form(request.form, items)
            errors.extend(financial_errors)
            if not errors:
                items['_amounts'] = amounts_data
                merged = amounts_placeholder_values(items)
                summaries = {
                    's1-21':merged['[MONTHLY RENT INITIAL]'] + ' (' + merged['[MONTHLY RENT INITIAL WORDS]'] + ')',
                    's1-22':merged['[S1 PARKING RENT PER BAY]'],
                    's1-23':'Agreed six-month adjustment amounts; no percentage escalation assumed.',
                    's1-24':'\n'.join(merged['[RENT ADJUSTMENT ' + str(i) + ' DATE]'] + ': ' + merged['[RENT ADJUSTMENT ' + str(i) + ' AMOUNT]'] for i in (1,2,3)),
                    's1-28':merged['[DEPOSIT AMOUNT]'],
                }
                for key,value in summaries.items():items[key] = {'value':value}
        if ancillary_submission:
            ancillary_data, ancillary_errors = banking_utilities_from_form(request.form)
            errors.extend(ancillary_errors)
            if not errors:
                ancillary_data['confirmed_by'] = int(current_user.id)
                ancillary_data['confirmed_at'] = datetime.now(timezone.utc).isoformat()
                items['_banking_utilities'] = ancillary_data
                merged = banking_utilities_placeholder_values(items)
                items['s1-36'] = {'value':merged['[SHARED UTILITY ALLOCATION METHOD]']}
                items['s1-38'] = {'value':merged[BANKING_TOKEN]}
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
        if group == 'period':
            automatic=request.form.get('period_form_version')=='2'
            if automatic:
                calculated,period_errors=calculated_period(request.form,items)
                errors.extend(period_errors)
                if not period_errors:
                    items['_period']=dict(items.get('_period',{}),**calculated)
                    items['s1-17']={'value':str(calculated['duration_months'])+' months'}
                    items['s1-18']={'value':calculated['commencement_date']}
                    items['s1-20']={'value':calculated['expiry_date']}
                    if 's1-19' in request.form:items['s1-19']={'value':request.form['s1-19'].strip()}
            errors += period_alerts(items, require_complete=not automatic)
        if not errors:
            if draft:save_items(draft, items)
            else:draft=create_parties_draft(property_id,items)
            flash('Lease particulars saved successfully. Lease remains a draft.' if group=='parties' else 'Lease particulars saved. This does not approve or activate the lease.', 'success')
            return redirect(url_for('billing_bp.lease_particulars', property_id=property_id, draft_id=draft['id']))
    return render_pm_template('program_billing/lease_particular_group.html', property=prop, lease_draft=draft,
        period_months=(request.form.get('s1-17','') if request.method=='POST' else period_duration_months(items)) if group=='period' else '', unavailable=reason, group=group, title=GROUPS[group][0], fields=fields, items=items, errors=errors,
        first_save_available=group=='parties' and not draft and not reason,
        first_save_message='Use Add / Select Tenants to save the contracting tenants, then save Part A.',
        draft_registration_token=(str(draft['registration_token']) if draft else request.form.get('registration_token') or str(uuid.uuid4())) if group=='parties' else None,
        lessors=list(candidates.values()), selected_lessor_ids=selected_ids, additional_text=additional_text, next_clause=MANIFEST['last_core_clause']+1,
        party_data=parties_state({'_parties':party_data}) if group=='parties' else party_data, party_types=PARTY_TYPES, premises_data=premises_data, premises_fields=PREMISES_FIELDS, amounts_data=amounts_data, ancillary_data=ancillary_data, ancillary_fields=ANCILLARY_FIELDS,
        owner_registry=owners, owner_preview=owner_parties_preview(owners, items, request.form if request.method=='POST' else None),
        owner_capacity_proposal=OWNER_CAPACITY_PROPOSAL,
        form1_values=party_placeholder_values(items) if group=='parties' else {},
        form1_saved_parties=parties_state(items) if group=='parties' else {},
        selected_tenants=[dict(p,id=p.get('tenant_id')) for p in selected_party_snapshots(items)] if group=='parties' else [],
        tenant_readiness=tenant_register.readiness() if group=='parties' else None,
        has_registered_tenants=bool(tenant_register.rows()) if group=='parties' else False,
        owner_read_reason=getattr(g,'pm_owner_read_unavailable_reason','')), (400 if errors else 200)


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
    items = draft['items'] if draft else {}
    issues = stage5_completion(items)
    issues.extend(('conditions',key,message) for key,message in reference_alerts(items))
    alerts = []
    for group, key, message in issues:
        alerts.append({'message':message, 'url':group_url(property_id, group, draft) + '#' + key})
    warnings = [{'message':message, 'url':url_for('billing_bp.lease_supporting', property_id=property_id, **({'draft_id':draft['id']} if draft else {})) + '#' + key}
                for group,key,message in completion_alerts(items, snapshot.documents(draft)) if group=='supporting']
    if items.get('_additional_clauses') or any(items.get('sc-' + str(i).zfill(2),{}).get('value') for i in range(1,11)):
        warnings.append({'message':'Additional conditions are recorded but have no slot in this pinned master; resolve them before generation.', 'url':group_url(property_id, 'conditions', draft)})
    master_problem = None
    try:
        from app.program_billing.lease_generation import load_master
        load_master()
    except (OSError,ValueError):master_problem = 'The pinned Stage 5 master is unavailable or failed integrity verification.'
    return render_pm_template('program_billing/lease_review.html', property=prop, lease_draft=draft,
        unavailable=reason, alerts=alerts, warnings=warnings, master_problem=master_problem,
        generation_ready=bool(draft and not reason and not alerts and not master_problem),
        parties_notice_warning=PARTIES_NOTICE_WARNING if parties_missing_notice_address(items) else None,
        parties_capacity_warning=OWNER_CAPACITY_WARNING if OWNER_CAPACITY_WARNING in parties_generation_warnings(items) else None)


@billing_bp.post('/billing/leases/property/<int:property_id>/generate', endpoint='lease_generate_review')
@snapshot.endpoint
def generate_review(property_id):
    prop, reason, draft = selected_context(property_id, True)
    snapshot.require_particulars_schema()
    if not draft:abort(400, description='Select a persistent draft first.')
    if request.form.get('review_draft_confirmation')!='on':
        abort(400, description='Confirm this document is a review draft, not for signature or issuance.')
    issues = stage5_completion(draft['items'])
    if issues:abort(400, description='Resolve the linked Stage 5 particulars checks before generation.')
    from app.program_billing.lease_generation import generate_review as build_review, MASTER_VERSION
    values = stage5_placeholder_values(draft['items'])
    try:generated = build_review(values, incomplete_particulars=bool(parties_generation_warnings(draft['items'])), particulars_warnings=parties_generation_warnings(draft['items']))
    except (OSError,ValueError):abort(503, description='Draft generation or master integrity verification failed; no generation was saved.')
    items = dict(draft['items'])
    history = list(items.get('_generations', []))
    if len(history)>=100:abort(409, description='Generation history limit reached; preserve this history and request an authorized review.')
    history.append({'id':str(uuid.uuid4()),'created_by':int(current_user.id),
        'created_at':datetime.now(timezone.utc).isoformat(),'source_draft_version':draft['version'],
        'master_version':MASTER_VERSION,'master_sha256':generated['master_sha256'],
        'output_sha256':generated['output_sha256'],'merged_values':dict(values),
        'status':generated['status'],'incomplete_particulars':generated.get('incomplete_particulars',False),
        'warnings':generated.get('warnings',[]),'signature_or_issuance_approved':False})
    items['_generations'] = history
    items['_master'] = {'version':MASTER_VERSION,'sha256':generated['master_sha256']}
    save_items(draft, items)  # Optimistic version check and commit before sending a successful download.
    response = send_file(BytesIO(generated['docx']),
        mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,download_name=f'lease-{property_id}-{draft["id"]}-DRAFT-FOR-REVIEW.docx',
        conditional=False,etag=False,max_age=0)
    response.headers['Cache-Control']='private, no-store'
    response.headers['X-Content-Type-Options']='nosniff'
    return response


@billing_bp.after_request
def protect_workflow(response):
    if request.endpoint in {'billing_bp.lease_particulars', 'billing_bp.lease_particular_group',
            'billing_bp.lease_master', 'billing_bp.lease_supporting', 'billing_bp.lease_review', 'billing_bp.lease_generate_review'}:
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


PARTY_TYPES = ('individual', 'company', 'trust', 'npc', 'other', 'juristic')
STRUCTURED_PARTY_KEYS = {'s1-03','s1-04','s1-05','s1-06','s1-07','s1-08','s1-09','s1-41'}
PARTIES_MASTER_SHA256 = '98b391c497cb2c70f931a6117a3b0074776256faaa8be91f3b6b9f4ad5d12937'
SIGNATURE_TOKENS = {
    '[LANDLORD SIGNATURE PLACE]':'landlord_place', '[LANDLORD SIGNATURE DATE]':'landlord_date',
    '[TENANT SIGNATURE PLACE]':'tenant_place', '[TENANT SIGNATURE DATE]':'tenant_date',
}
PARTY_FIELDS = {
    'lessor': {'signatory_name':200, 'signatory_identity_number':80, 'signatory_capacity':200, 'service_address':2000, 'email':255, 'phone':50},
    'lessee': {'legal_name':200, 'entity_type':16, 'identity_number':80, 'email':255, 'phone':50,
               'physical_address':2000, 'service_address':2000, 'signatory_name':200, 'signatory_identity_number':80, 'signatory_capacity':200, 'party_type':16, 'address':2000},
    'signatures': {'landlord_place':200, 'landlord_date':10, 'tenant_place':200, 'tenant_date':10},
}


def selected_party_snapshots(items):
    parties=items.get('_parties',{})
    if 'lessees' in parties:return parties['lessees']
    legacy=parties.get('lessee',{})
    return [legacy] if legacy.get('tenant_id') else []


def set_selected_tenants(items, records):
    """Only the existing Add / Select Tenants route calls this JSON selection writer."""
    snapshots=[]
    for record in records:
        form={key:record.get(key,'') or '' for key in tenant_register.FIELDS}
        form.update(representative_name=record.get('signatory_name') or '',
            representative_identity_number=record.get('signatory_identity_number') or '')
        _,errors=tenant_register.validate(form)
        if errors:abort(400,description=record['legal_name']+': '+' '.join(errors))
        party={key:record.get(key,'') or '' for key in PARTY_FIELDS['lessee']}
        party.update(entity_type='individual' if record['party_type']=='natural_person' else 'juristic',
            physical_address=record['address'],service_address=record['address'])
        snapshots.append(tenant_snapshot_details(party,record))
    parties=deepcopy(items.get('_parties',{}))
    parties.update(schema_version=3,master_sha256=PARTIES_MASTER_SHA256,
        lessee=deepcopy(snapshots[0]),lessees=snapshots)
    items['_parties']=parties
    values=party_placeholder_values(items)
    for key,token in [('s1-06','[S1 LEGAL TENANT]'),('s1-07','[TENANT REGISTRATION OR ID]'),
                     ('s1-08','[TENANT SIGNATORY NAME AND CAPACITY]'),('s1-09','[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]')]:
        items[key]={'value':values[token]}
    return items


def tenant_snapshot_details(party, record):
    result=deepcopy(party)
    result.update(tenant_id=record['id'],identity_source='bil_tenant')
    for key in ('representative_id','representative_phone','representative_email'):result[key]=record[key]
    return result


def selected_tenant_ids(items):
    saved=items.get('_parties',{})
    if 'lessees' in saved:
        selected=[str(p['tenant_id']) for p in saved['lessees'] if p.get('tenant_id')]
    else:
        selected=[str(saved['lessee']['tenant_id'])] if saved.get('lessee',{}).get('tenant_id') else []
    if request.method=='POST':
        if request.form.get('parties_form_version')=='5':return request.form.getlist('tenant_ids')
        if request.form.get('parties_form_version')=='4':return request.form.getlist('tenant_id')
    added=request.args.get('tenant_id')
    if added and added not in selected:selected.append(added)
    return selected


def tenant_snapshot_errors(items):
    saved=items.get('_parties',{})
    if 'lessees' not in saved:return []
    tenants=saved['lessees'];errors=[];seen=set()
    if not isinstance(tenants,list) or not tenants:return ['Select at least one registered tenant.']
    state=parties_state(items)
    for party in tenants:
        if not isinstance(party,dict):errors.append('Invalid saved tenant particulars.');continue
        identifier=party.get('tenant_id')
        if type(identifier) is not int or identifier<=0 or identifier in seen:errors.append('Saved tenants require distinct registered tenant IDs.')
        if type(identifier) is int:seen.add(identifier)
        form={role+'_'+key:value for role in ('lessor','lessee') for key,value in state[role].items()}
        form.update({'lessee_'+key:party.get(key,'') for key in PARTY_FIELDS['lessee']})
        _,messages=simplified_parties_from_form(form,items,require_lessor_capacity=False,require_lessee_capacity=False)
        errors.extend((party.get('legal_name') or 'Tenant')+': '+message for message in messages if not message.startswith('Lessor '))
    return errors


def parties_state(items):
    """Preserve old free text without guessing registration or signatory identities."""
    saved = items.get('_parties', {})
    state = {role:{key:saved.get(role, {}).get(key, '') for key in fields} for role, fields in PARTY_FIELDS.items()}
    if not saved:
        state['lessee']['legal_name'] = items.get('s1-06', {}).get('value', '')
        state['lessor']['service_address'] = items.get('s1-05', {}).get('value', '')
        state['lessee']['service_address'] = items.get('s1-09', {}).get('value', '')
    state['lessee']['party_type'] = state['lessee']['party_type'] or ('natural_person' if state['lessee']['entity_type'] == 'individual' else 'juristic_person' if state['lessee']['entity_type'] else '')
    state['lessee']['address'] = state['lessee']['address'] or state['lessee']['service_address'] or state['lessee']['physical_address']
    return state


def simplified_parties_from_form(form, existing, require_lessor_capacity=True, require_lessee_capacity=True):
    state = deepcopy(existing.get('_parties', {}))
    state.update(schema_version=2, master_sha256=PARTIES_MASTER_SHA256)
    current = parties_state(existing)
    for role in PARTY_FIELDS:state[role] = dict(current[role])
    editable = {
        'lessor':('signatory_name','signatory_identity_number','signatory_capacity','service_address'),
        'lessee':('legal_name','party_type','identity_number','address','email','phone'),
    }
    errors = []
    for role, keys in editable.items():
        for key in keys:state[role][key] = form.get(role+'_'+key, '').strip()
    kind = state['lessee']['party_type']
    if kind not in ('natural_person','juristic_person'):errors.append('Choose Natural person or Juristic person.')
    if kind == 'natural_person':
        state['lessee']['entity_type'] = 'individual'
        # The natural person contracts and signs personally, without a representative.
        for key in ('signatory_name','signatory_identity_number','signatory_capacity'):state['lessee'][key] = ''
    elif kind == 'juristic_person':
        state['lessee']['entity_type'] = current['lessee']['entity_type'] if current['lessee']['entity_type'] in ('company','trust','npc','other','juristic') else 'juristic'
        for key in ('signatory_name','signatory_identity_number','signatory_capacity'):
            state['lessee'][key] = form.get('lessee_'+key, '').strip()
    state['lessee']['physical_address'] = state['lessee']['address']
    required = {'lessor':('signatory_name','signatory_identity_number') + (('signatory_capacity',) if require_lessor_capacity else ()),
                'lessee':('legal_name','identity_number','address')}
    if kind == 'juristic_person':required['lessee'] += ('signatory_name','signatory_identity_number') + (('signatory_capacity',) if require_lessee_capacity else ())
    labels = {'signatory_name':'representative full name','signatory_identity_number':'representative ID number',
              'signatory_capacity':'representative capacity','legal_name':'legal name','identity_number':'ID / registration number','address':'address'}
    for role,keys in required.items():
        for key in keys:
            if not state[role][key]:errors.append(role.title()+' '+labels[key]+' is required.')
    for role,fields in PARTY_FIELDS.items():
        for key,limit in fields.items():
            value = state[role][key]
            if len(value)>limit:errors.append(role.title()+' '+key.replace('_',' ')+': maximum length is '+str(limit)+'.')
            if re.search(r'\[[^\[\]]+\]',value):errors.append('Replace unresolved placeholders with actual party details.')
    email = state['lessee']['email']
    if email and not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+',email):errors.append('Lessee email address is invalid.')
    return state, errors


def parties_from_form(form, existing=None, owner_registry=None, tenant_record=None):
    if form.get('parties_form_version') == '3':return registered_owner_parties_from_form(form, existing or {}, owner_registry, tenant_record)
    if form.get('parties_form_version') == '2':return simplified_parties_from_form(form, existing or {})
    state = {'schema_version':1, 'master_sha256':PARTIES_MASTER_SHA256}
    errors = []
    if form.get('parties_form_version') != '1':errors.append('Reload the Parties form before saving structured details.')
    for role, fields in PARTY_FIELDS.items():
        state[role] = {}
        for key, limit in fields.items():
            value = form.get(role + '_' + key, '').strip()
            state[role][key] = value
            if len(value) > limit:errors.append(role.title() + ' ' + key.replace('_',' ') + ': maximum length is ' + str(limit) + '.')
    for role, keys in {'lessor':('signatory_name','signatory_capacity','service_address'),
                       'lessee':('legal_name','entity_type','identity_number','physical_address','service_address','signatory_name','signatory_capacity')}.items():
        for key in keys:
            if not state[role][key]:errors.append(role.title() + ' ' + key.replace('_',' ') + ' is required.')
    if state['lessee']['entity_type'] not in PARTY_TYPES:errors.append('Choose a valid lessee entity type.')
    for role in ('lessor','lessee'):
        email = state[role]['email']
        if email and not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+',email):errors.append(role.title() + ' email address is invalid.')
    for key in ('landlord_date','tenant_date'):
        value = state['signatures'][key]
        if value:
            try:
                if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
                date.fromisoformat(value)
            except ValueError:errors.append(key.replace('_',' ').title() + ' must be a valid date or left blank.')
    for role in PARTY_FIELDS:
        for value in state[role].values():
            if re.search(r'\[[^\[\]]+\]',value):errors.append('Replace unresolved placeholders with actual party details; signature details may remain blank.')
    return state, errors


def party_placeholder_values(items):
    """Exact Stage 5 party tokens only; blanks remain blanks, never guessed dates."""
    state = parties_state(items)
    simplified = items.get('_parties', {}).get('schema_version') in (2,3)
    def signatory(role):
        if simplified and role == 'lessee' and state['lessee']['party_type'] == 'natural_person':
            return state['lessee']['legal_name'] + ' - In personal capacity'
        name = state[role]['signatory_name']
        identity = state[role]['signatory_identity_number']
        if identity:name += ' (ID number: ' + identity + ')'
        capacity = state[role]['signatory_capacity']
        return ' - '.join(value for value in (name,capacity) if value)
    def service(role):
        address = state[role]['service_address']
        if simplified:
            address = state[role]['address'] if role == 'lessee' else address or '________________'
        return '\n'.join(value for value in (address,state[role]['email'],state[role]['phone']) if value)
    def owner_name(owner):
        return owner['name'] + (' - ID / registration number: ' + owner['identity_number'] if owner.get('identity_number') else '')
    values = {
        '[S1 LEGAL LANDLORD(S)]':'\n'.join(owner_name(owner) for owner in items.get('_contracting_lessors',{}).get('identities',[])),
        '[LANDLORD SIGNATORY NAME AND CAPACITY]':signatory('lessor'),
        '[S1 LEGAL TENANT]':state['lessee']['legal_name'],
        '[TENANT REGISTRATION OR ID]':state['lessee']['identity_number'],
        '[TENANT SIGNATORY NAME AND CAPACITY]':signatory('lessee'),
        '[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]':service('lessor'),
        '[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]':service('lessee'),
    }
    tenants = items.get('_parties',{}).get('lessees',[])
    if len(tenants)>1:
        values['[S1 LEGAL TENANT]'] = '\n'.join(p['legal_name'] for p in tenants)
        values['[TENANT REGISTRATION OR ID]'] = '\n'.join(
            p['legal_name']+' - '+('ID Number: ' if p['party_type']=='natural_person' else 'Registration Number: ')+p['identity_number'] for p in tenants)
        representatives=[]
        for p in tenants:
            if p['party_type']=='natural_person':
                representatives.append(p['legal_name']+' (ID Number: '+p['identity_number']+') - In personal capacity')
            else:
                representative=p['signatory_name']+' (ID Number: '+p['signatory_identity_number']+')'
                if p.get('signatory_capacity'):representative+=' - '+p['signatory_capacity']
                representatives.append(p['legal_name']+' - Represented by '+representative)
        values['[TENANT SIGNATORY NAME AND CAPACITY]']='\n'.join(representatives)
        values['[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]']='\n\n'.join(
            p['legal_name']+':\n'+'\n'.join(v for v in (p['address'],p['email'],p['phone']) if v) for p in tenants)
    values.update({token:state['signatures'][field] for token,field in SIGNATURE_TOKENS.items()})
    return values


PARTIES_NOTICE_WARNING = 'Lessor service address not supplied \u2014 complete before signing.'


def parties_missing_notice_address(items):
    return items.get('_parties', {}).get('schema_version') in (2,3) and not parties_state(items)['lessor']['service_address'].strip()


OWNER_CAPACITY_PROPOSAL = 'Owner representing all Lessors'
OWNER_CAPACITY_WARNING = 'Lessor representative capacity not confirmed \u2014 review before signing.'


def registry_owner_snapshot(owner):
    return {key:owner.get(key) for key in ('owner_id','type','name','identity_number','email','phone','residential_address','postal_address')}


def matching_owner_capacity(owner, items):
    saved = items.get('_parties',{}).get('lessor',{})
    # Never infer identity from a shared name or reuse another person's capacity.
    identity = (owner.get('identity_number') or '').strip()
    if identity and identity == (saved.get('signatory_identity_number') or '').strip():
        return saved.get('signatory_capacity') or ''
    return ''


def owner_parties_preview(owners, items, form=None):
    saved = items.get('_parties',{}).get('lessor',{})
    selected = str(saved.get('owner_id') or '')
    if not selected:
        matches=[o for o in owners if o.get('type')=='individual' and o.get('identity_number') and o.get('identity_number')==saved.get('signatory_identity_number')]
        if len(matches)==1:selected=str(matches[0]['owner_id'])
    if form is not None and form.get('parties_form_version') in ('3','4','5','6'):selected=form.get('representative_owner_id','')
    owner=next((o for o in owners if str(o['owner_id'])==selected),{})
    confirmed=bool(owner and owner.get('type')=='individual' and saved.get('capacity_confirmed') and str(saved.get('owner_id'))==selected)
    if form is not None and form.get('parties_form_version')=='3':confirmed=form.get('confirm_representative_capacity')=='on'
    return {'selected':selected,'owner':owner,'capacity':matching_owner_capacity(owner,items), 'confirmed':confirmed,
            'capacities':{str(o['owner_id']):matching_owner_capacity(o,items) for o in owners}}


def registered_owner_parties_from_form(form, existing, owner_registry=None, tenant_record=None):
    from werkzeug.datastructures import MultiDict
    owners = owner_registry if owner_registry is not None else existing.get('_contracting_lessors',{}).get('identities',[])
    selected = form.getlist('representative_owner_id') if hasattr(form,'getlist') else [form.get('representative_owner_id','')]
    errors=[]
    if len(selected)!=1 or not selected[0]:errors.append('Select exactly one individual owner as representative of all Lessors.')
    owner=next((o for o in owners if str(o['owner_id'])==(selected[0] if len(selected)==1 else '')),None)
    if owner is None or owner.get('type')!='individual':
        errors.append('The representative must be an associated individual owner. Entity-owner delegate details are not available in the owner register.')
        owner={}
    if not owners:errors.append('No current property owners are available. Register or associate owners in Property Management first.')
    for record in owners:
        if not record.get('name') or not record.get('identity_number'):errors.append('An associated owner is missing a legal name or ID / registration number. Update the existing owner record; no information was invented.')
    capacity=matching_owner_capacity(owner,existing)
    confirmed=False
    forwarded=MultiDict(form)
    forwarded['parties_form_version']='2'
    forwarded['lessor_signatory_name']=owner.get('name') or ''
    forwarded['lessor_signatory_identity_number']=owner.get('identity_number') or ''
    forwarded['lessor_signatory_capacity']=capacity
    forwarded['lessor_service_address']=owner.get('residential_address') or ''
    state,messages=simplified_parties_from_form(forwarded,existing,require_lessor_capacity=False,
        require_lessee_capacity=not (tenant_record or existing.get('_parties',{}).get('lessee',{}).get('identity_source')=='bil_tenant'))
    state['schema_version']=3
    state['lessor'].update(owner_id=owner.get('owner_id'),email=owner.get('email') or '',phone=owner.get('phone') or '',
        capacity_proposal=OWNER_CAPACITY_PROPOSAL,capacity_confirmed=confirmed,
        address_source='owner.residential_address',identity_source='property_owner_register')
    return state,errors+messages


def record_representative_change(items, proposed):
    old=items.get('_parties',{}).get('lessor',{})
    if not old or (old.get('signatory_identity_number'),old.get('signatory_name'))==(proposed['lessor'].get('signatory_identity_number'),proposed['lessor'].get('signatory_name')):return False
    # Existing metadata records approval, not proof of external delivery. Preserve
    # the original whenever it is marked signature/issuance-approved.
    if not any(g.get('signature_or_issuance_approved') is True for g in items.get('_generations',[])):return False
    requests=list(items.get('_representative_change_requests',[]))
    requests.append({'id':str(uuid.uuid4()),'created_by':int(current_user.id),'created_at':datetime.now(timezone.utc).isoformat(),
        'original_representative':deepcopy(old),'proposed_representative':deepcopy(proposed['lessor']),
        'basis':'recorded_signature_or_issuance_approval','status':'pending_review',
        'addendum_required':True,'lessee_notice_required':True,'addendum_issued':False,'notice_sent':False})
    items['_representative_change_requests']=requests
    return True


def parties_generation_warnings(items):
    warnings=[PARTIES_NOTICE_WARNING] if parties_missing_notice_address(items) else []
    # Owner selection does not invent representative authority or capacity.
    return warnings


# Stage 5 Premises mapping supplements, rather than renumbers, existing R01 keys.
STRUCTURED_PREMISES_KEYS = {'s1-10','s1-11','s1-12','s1-13','s1-14'}
PREMISES_FIELDS = {
    'property_name': ('Property identification / name', 200),
    'property_address': ('Property physical address', 2000),
    'property_legal_description': ('Title / cadastral description (optional)', 2000),
    'property_extent_sqm': ('Whole-site property extent in square metres', 30),
    'leased_area_sqm': ('Leased premises area in square metres', 30),
    'unit_count': ('Number of leased units / portions', 9),
    'leased_portion_description': ('Exact leased premises / included units or portions', 2000),
    'excluded_areas': ('Excluded areas (enter None where agreed)', 2000),
    'plan_a_identifier': ('Plan A site plan identifier and drawing revision', 200),
    'plan_b_identifier': ('Plan B premises / unit layout identifier and drawing revision', 200),
}
PREMISES_TOKENS = {
    '[S1 PROPERTY NAME]':'property_name',
    '[S1 PREMISES ADDRESS]':'property_address',
    '[S1 EXTENT]':'property_extent_sqm',
    '[LEASED PREMISES AREA]':'leased_area_sqm',
    '[S1 UNIT COUNT]':'unit_count',
    '[PLAN A IDENTIFIER]':'plan_a_identifier',
    '[PLAN B IDENTIFIER]':'plan_b_identifier',
}


def premises_state(items, prop=None):
    saved = items.get('_premises', {})
    state = {key:saved.get(key, '') for key in PREMISES_FIELDS}
    state['lease_scope'] = saved.get('lease_scope', '')
    if not saved:
        # Reuse authorized register details only; do not infer legal/leased boundaries.
        prop = prop or {}
        state['property_name'] = items.get('s1-10', {}).get('value', prop.get('name', ''))
        state['property_address'] = items.get('s1-11', {}).get('value', prop.get('address') or '')
    return state


def premises_from_form(form, existing=None):
    if form.get('premises_form_version')=='2':
        state=deepcopy(existing or {})
        state.setdefault('schema_version',1)
        state.setdefault('master_sha256',PARTIES_MASTER_SHA256)
        errors=[]
        for key in ('property_name','property_address'):
            label,limit=PREMISES_FIELDS[key]
            value=form.get('premises_'+key,'').strip()
            if not value:errors.append(label+' is required.')
            if len(value)>limit:errors.append(label+': maximum length is '+str(limit)+'.')
            if re.search(r'\[[^\[\]]+\]',value):errors.append(label+': replace unresolved placeholders with confirmed particulars.')
            state[key]=value
        return state,errors
    state = {'schema_version':1, 'master_sha256':PARTIES_MASTER_SHA256}
    errors = []
    if form.get('premises_form_version') != '1':errors.append('Reload the Premises form before saving structured details.')
    for key, (label, limit) in PREMISES_FIELDS.items():
        value = form.get('premises_' + key, '').strip()
        state[key] = value
        if not value and key != 'property_legal_description':errors.append(label + ' is required.')
        if len(value) > limit:errors.append(label + ': maximum length is ' + str(limit) + '.')
        if re.search(r'\[[^\[\]]+\]',value):errors.append(label + ': replace unresolved placeholders with confirmed particulars.')
    state['lease_scope'] = form.get('premises_lease_scope', '').strip()
    if state['lease_scope'] not in ('whole_property','part_only'):
        errors.append('Explicitly choose whether the whole property or only part is leased.')
    for key in ('property_extent_sqm','leased_area_sqm'):
        value = state[key]
        if not re.fullmatch(r'[0-9]{1,12}(?:\.[0-9]{1,6})?',value):
            errors.append(PREMISES_FIELDS[key][0] + ': enter a positive number without units or separators.')
            continue
        try:
            if Decimal(value) <= 0:raise InvalidOperation()
        except InvalidOperation:errors.append(PREMISES_FIELDS[key][0] + ' must be greater than zero.')
    if not re.fullmatch(r'[1-9][0-9]{0,8}',state['unit_count']):
        errors.append('Number of leased units / portions must be a positive whole number.')
    # Site area and floor area can differ (including multiple floors): never equate them.
    return state, errors


def premises_placeholder_values(items):
    state = premises_state(items)
    return {token:state[field] for token, field in PREMISES_TOKENS.items()}


PERIOD_TOKENS = {
    '[COMMENCEMENT DATE]':'s1-18',
    '[OCCUPATION DATE]':'s1-19',
    '[EXPIRY DATE]':'s1-20',
}


def calendar_month_date(value,months,preserve_month_end=False):
    year,month=divmod(value.year*12+value.month-1+months,12)
    if not 1<=year<=9999:raise ValueError('Date is outside the supported calendar range.')
    last=calendar.monthrange(year,month+1)[1]
    day=last if preserve_month_end and value.day==calendar.monthrange(value.year,value.month)[1] else min(value.day,last)
    return date(year,month+1,day)


def calculated_period(form,items):
    errors=[];raw=form.get('s1-17','').strip();start=form.get('s1-18','').strip()
    if not re.fullmatch(r'[0-9]{1,4}',raw) or not 1<=int(raw or '0')<=1200:
        errors.append('Lease duration must be a whole number between 1 and 1200 months.')
    try:
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',start):raise ValueError()
        commencement=date.fromisoformat(start)
    except ValueError:errors.append('Enter a valid lease commencement date (YYYY-MM-DD).')
    if errors:return {},errors
    try:
        expiry=calendar_month_date(commencement,int(raw))-timedelta(days=1)
        notice=calendar_month_date(expiry,-3,preserve_month_end=True)
    except (ValueError,OverflowError):return {},['Lease dates exceed the supported calendar range.']
    return {'duration_months':int(raw),'commencement_date':start,'expiry_date':expiry.isoformat(),'renewal_notice_date':notice.isoformat()},[]


def period_duration_months(items):
    raw=items.get('s1-17',{}).get('value','').strip()
    match=re.fullmatch(r'(\d+)\s*(months?|years?)?',raw,re.I)
    return str(int(match[1])*(12 if (match[2] or '').lower().startswith('year') else 1)) if match else ''


def period_field(field):
    """Display the approved Stage 5 mapping without rewriting the R01 catalog."""
    result = dict(field)
    if field['key'] == 's1-17':
        result.update(label='Lease Duration (Months)', requirement='Essential', placeholders=[])
    elif field['key'] in PERIOD_TOKENS.values():
        labels = {'s1-18':'Lease commencement date','s1-19':'Physical occupation date','s1-20':'Lease expiry date'}
        result.update(label=labels[field['key']], placeholders=[token for token,key in PERIOD_TOKENS.items() if key==field['key']])
    result['placeholder_source'] = 'Stage 5 master'
    return result


def period_placeholder_values(items):
    return {token:items.get(key, {}).get('value', '').strip() for token,key in PERIOD_TOKENS.items()}


FINANCIAL_KEYS = ('monthly_rent','monthly_rent_words','rent_effective_date','deposit_amount',
                  'parking_applicability','parking_bay_count','parking_bay_identifiers','parking_rent_per_bay','parking_details') + tuple(
                  'adjustment_' + str(i) + '_' + field for i in (1,2,3) for field in ('applicability','date','amount'))
FINANCIAL_TOKENS = {
    '[MONTHLY RENT INITIAL]':'monthly_rent', '[MONTHLY RENT INITIAL WORDS]':'monthly_rent_words',
    '[RENT EFFECTIVE DATE]':'rent_effective_date', '[DEPOSIT AMOUNT]':'deposit_amount',
    '[PARKING BAY COUNT]':'parking_bay_count', '[PARKING BAY IDENTIFIERS]':'parking_bay_identifiers',
    '[S1 PARKING RENT PER BAY]':'parking_rent_per_bay',
    **{'[RENT ADJUSTMENT ' + str(i) + ' ' + kind.upper() + ']':'adjustment_' + str(i) + '_' + kind
       for i in (1,2,3) for kind in ('date','amount')},
}


def amounts_state(items):
    saved = items.get('_amounts', {})
    state={key:saved.get(key, '') for key in FINANCIAL_KEYS}
    if 'parking_details' not in saved:state['parking_details']=saved.get('parking_bay_identifiers','')
    return state


def zar_amount(raw):
    # Explicit plain decimal ZAR; never interpret commas, negatives or exponent notation.
    if not re.fullmatch(r'[0-9]{1,12}(?:\.[0-9]{1,2})?',raw):
        raise ValueError('Enter a non-negative ZAR amount with at most two decimal places, without symbols or separators.')
    return Decimal(raw).quantize(Decimal('0.01'))


def zar_words(raw):
    amount=zar_amount(raw)
    ones=('zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen').split()
    tens=('zero ten twenty thirty forty fifty sixty seventy eighty ninety').split()
    def below_thousand(n):
        parts=[]
        if n>=100:parts.append(ones[n//100]+' hundred');n%=100
        if n:
            tail=ones[n] if n<20 else tens[n//10]+('-'+ones[n%10] if n%10 else '')
            parts.append(tail)
        return ' and '.join(parts) if parts else 'zero'
    def integer(n):
        if not n:return 'zero'
        groups=[]
        for value,label in ((10**9,'billion'),(10**6,'million'),(1000,'thousand')):
            count,n=divmod(n,value)
            if count:groups.append(below_thousand(count)+' '+label)
        if n:groups.append(('and ' if groups and n<100 else '')+below_thousand(n))
        return ' '.join(groups)
    whole=int(amount);cents=int((amount-whole)*100)
    return integer(whole)+' rand and '+integer(cents)+(' cent' if cents==1 else ' cents')


def financial_date(raw):
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',raw):raise ValueError('Enter YYYY-MM-DD.')
    return date.fromisoformat(raw)


def six_month_date(anchor, periods):
    year, month = divmod(anchor.year * 12 + anchor.month - 1 + 6 * periods, 12)
    if year > 9999:raise ValueError('Adjustment exceeds supported calendar range.')
    return date(year, month + 1, min(anchor.day,calendar.monthrange(year,month + 1)[1]))


def amounts_from_form(form, items):
    simplified=form.get('amounts_form_version')=='2'
    state = deepcopy(items.get('_amounts',{})) if simplified else {}
    state.update(schema_version=2 if simplified else 1,master_sha256=PARTIES_MASTER_SHA256,currency='ZAR')
    errors = []
    if form.get('amounts_form_version') not in ('1','2'):errors.append('Reload the Amounts form before saving financial details.')
    for key in FINANCIAL_KEYS:
        if simplified and key in ('monthly_rent_words','parking_bay_count','parking_bay_identifiers','parking_rent_per_bay'):
            state.setdefault(key,'');continue  # Preserve hidden historical parking values; wording is server-derived.
        state[key] = form.get('amounts_' + key, state.get(key,'') if simplified else '').strip()
        limit = 2000 if key in ('parking_bay_identifiers','parking_details') else 300 if key=='monthly_rent_words' else 40
        if len(state[key]) > limit:errors.append(key.replace('_',' ').title() + ': text is too long.')
        if re.search(r'\[[^\[\]]+\]',state[key]):errors.append(key.replace('_',' ').title() + ': replace unresolved placeholders.')
    numbers = {}
    for key in ('monthly_rent','deposit_amount'):
        try:numbers[key] = zar_amount(state[key])
        except ValueError as error:errors.append(key.replace('_',' ').title() + ': ' + str(error))
    if simplified and 'monthly_rent' in numbers:state['monthly_rent_words']=zar_words(state['monthly_rent'])
    if not state['monthly_rent_words']:errors.append('Monthly rental in words is required; confirm that it matches the numeric rental.')
    if all(key in numbers for key in ('monthly_rent','deposit_amount')) and numbers['monthly_rent'] != numbers['deposit_amount']:
        errors.append('Deposit must match one month of initial rental under clauses 13.1 and 13.1.1. Review the entered amount; no deposit was calculated or changed.')
    start = end = effective = None
    if period_alerts(items, require_complete=True):
        errors.append('Complete and validate Part 3 lease dates before recording the rental schedule.')
    else:
        start = financial_date(items['s1-18']['value']);end = financial_date(items['s1-20']['value'])
    try:effective = financial_date(state['rent_effective_date'])
    except ValueError:errors.append('Rental effective date must be a valid YYYY-MM-DD date.')
    if effective and start and end and not start <= effective <= end:
        errors.append('Rental effective date must fall within the lease period; it is not assumed equal to commencement or occupation.')
    for i in (1,2,3):
        prefix = 'adjustment_' + str(i) + '_'
        applicable = state[prefix+'applicability']
        expected = None
        if effective:
            try:expected = six_month_date(effective,i)
            except ValueError:errors.append('Adjustment ' + str(i) + ': date exceeds supported calendar range.')
        if applicable not in ('applies','after_expiry'):
            errors.append('Adjustment ' + str(i) + ': explicitly choose Applies or After expiry.')
            continue
        if applicable=='after_expiry':
            if not expected or not end or expected <= end:
                errors.append('Adjustment ' + str(i) + ': Not Applicable is allowed only when its six-month date is after expiry.')
            if state[prefix+'date'] or state[prefix+'amount']:
                errors.append('Adjustment ' + str(i) + ': leave date and amount blank when explicitly after expiry.')
            continue
        try:
            entered = financial_date(state[prefix+'date'])
            if expected and entered != expected:errors.append('Adjustment ' + str(i) + ': date must follow the master six-month schedule (' + str(expected) + ').')
            if start and end and not start <= entered <= end:errors.append('Adjustment ' + str(i) + ': date must fall within the lease period.')
        except ValueError:errors.append('Adjustment ' + str(i) + ': enter a valid YYYY-MM-DD date.')
        try:zar_amount(state[prefix+'amount'])
        except ValueError as error:errors.append('Adjustment ' + str(i) + ' amount: ' + str(error))
    if effective and end:
        try:
            if six_month_date(effective,4) <= end:
                errors.append('The lease requires more than the three adjustment rows in this master. Review the schedule; no extra provision or amount was invented.')
        except ValueError:pass  # No additional supported date can fall within the supported lease range.
    parking = state['parking_applicability']
    if simplified:
        if parking not in ('none','other'):errors.append('Select Not Applicable or Other for parking.')
        if parking=='other' and not state['parking_details']:errors.append('Parking Details are required when Other is selected.')
    elif parking=='allocated':
        if not re.fullmatch(r'[1-9][0-9]{0,3}',state['parking_bay_count']):errors.append('Allocated parking requires a positive whole bay count (maximum 9999).')
        identifiers = [line.strip() for line in state['parking_bay_identifiers'].splitlines() if line.strip()]
        if not identifiers or len(set(identifier.casefold() for identifier in identifiers)) != len(identifiers):errors.append('List distinct allocated bay identifiers, one per line.')
        if state['parking_bay_count'].isdigit() and len(identifiers) != int(state['parking_bay_count']):errors.append('Parking bay identifiers must match the allocated bay count.')
        try:zar_amount(state['parking_rent_per_bay'])
        except ValueError as error:errors.append('Parking rent per bay: ' + str(error))
    elif parking=='none':
        if state['parking_bay_count'] not in ('','0') or state['parking_bay_identifiers']:
            errors.append('No allocated parking: leave bay identifiers blank and bay count blank or zero.')
        if state['parking_rent_per_bay']:
            try:
                if zar_amount(state['parking_rent_per_bay']) != 0:errors.append('No allocated parking cannot have a non-zero parking rental.')
            except ValueError as error:errors.append('Parking rent per bay: ' + str(error))
    else:errors.append('Explicitly choose allocated parking or no allocated parking.')
    return state, errors


def amounts_placeholder_values(items):
    state = amounts_state(items)
    money = {'monthly_rent','deposit_amount','parking_rent_per_bay'} | {'adjustment_' + str(i) + '_amount' for i in (1,2,3)}
    values = {}
    for token,key in FINANCIAL_TOKENS.items():
        value = state[key]
        if key in money and value:value = 'R ' + format(zar_amount(value),',.2f').replace(',',' ')
        values[token] = value
    for i in (1,2,3):
        if state['adjustment_' + str(i) + '_applicability']=='after_expiry':
            values['[RENT ADJUSTMENT ' + str(i) + ' DATE]'] = 'Not applicable (after expiry)'
            values['[RENT ADJUSTMENT ' + str(i) + ' AMOUNT]'] = 'Not applicable'
    if state['parking_applicability']=='other':
        values['[PARKING BAY IDENTIFIERS]']=state['parking_details']
    if state['parking_applicability']=='none':
        values.update({'[PARKING BAY COUNT]':'0','[PARKING BAY IDENTIFIERS]':'Not applicable (no allocated bays)','[S1 PARKING RENT PER BAY]':'R 0.00'})
    return values


BANKING_TOKEN = '[S1 VERIFIED BANK, BRANCH, ACCOUNT HOLDER, ACCOUNT TYPE, ACCOUNT NUMBER AND SWIFT WHERE APPLICABLE]'
ANCILLARY_FIELDS = {
    'utility_allocation_method':('Shared-utility allocation arrangement',2000),
    'utility_references':('Meter / account references (where supplied)',2000),
    'utility_no_references_reason':('Reason no separate references are supplied',1000),
    'bank_name':('Bank name',200),
    'branch_name':('Branch name (optional if branch code supplied)',200),
    'branch_code':('South African branch code (six digits)',6),
    'account_holder':('Account holder',200),
    'account_number':('Account number (6?20 digits)',20),
    'account_type_other':('Other account type description',100),
    'swift':('SWIFT / BIC (optional where not applicable)',11),
}
ANCILLARY_KEYS = tuple(ANCILLARY_FIELDS) + ('utility_reference_mode','account_type')
ANCILLARY_TOKENS = (
    '[SHARED UTILITY ALLOCATION METHOD]',
    '[UTILITY METER OR ACCOUNT REFERENCES]',
    BANKING_TOKEN,
)


def banking_utilities_state(items):
    # Read only this lease's explicitly entered snapshot, never property/owner/global bank records.
    saved = items.get('_banking_utilities', {})
    state = {key:saved.get(key,'') for key in ANCILLARY_KEYS}
    state.update({key:saved.get(key) is True for key in ('utilities_confirmed','banking_confirmed')})
    return state


def banking_utilities_from_form(form):
    state = {'schema_version':1,'master_sha256':PARTIES_MASTER_SHA256}
    errors = []
    if form.get('part5_form_version') != '1':errors.append('Reload Banking and Utilities before saving.')
    for key in ANCILLARY_KEYS:
        value = form.get('part5_' + key,'').strip()
        state[key] = value
        limit = ANCILLARY_FIELDS[key][1] if key in ANCILLARY_FIELDS else 30
        if len(value) > limit:errors.append(key.replace('_',' ').title() + ': text is too long.')
        if re.search(r'\[[^\[\]]+\]',value):errors.append(key.replace('_',' ').title() + ': replace unresolved merge placeholders.')
        if any(ord(c)<32 and c not in '\n\r\t' for c in value):errors.append(key.replace('_',' ').title() + ': unsupported control characters.')
    for key in ('utilities_confirmed','banking_confirmed'):
        state[key] = form.get('part5_' + key)=='on'
        if not state[key]:errors.append('Explicitly confirm ' + ('the shared-utility arrangement.' if key=='utilities_confirmed' else 'the banking details and your authority to nominate them.'))
    if not state['utility_allocation_method']:errors.append('Enter the actual shared-utility arrangement, including an explicit statement where shared allocation does not apply.')
    mode = state['utility_reference_mode']
    if mode=='provided':
        if not state['utility_references']:errors.append('Enter the supplied meter / account references.')
        if state['utility_no_references_reason']:errors.append('Clear the no-reference explanation when references are provided.')
    elif mode=='none':
        if state['utility_references']:errors.append('No separate references: clear the reference field rather than keeping contradictory details.')
        if len(state['utility_no_references_reason'])<10:errors.append('Explain why no separate meter / account references are supplied (at least 10 characters).')
    else:errors.append('Explicitly select references supplied or no separate references supplied.')
    for key in ('bank_name','account_holder'):
        if not state[key]:errors.append(ANCILLARY_FIELDS[key][0] + ' is required.')
    if not re.fullmatch(r'[0-9]{6}',state['branch_code']):errors.append('Branch code must contain exactly six digits.')
    if not re.fullmatch(r'[0-9]{6,20}',state['account_number']):errors.append('Account number must contain 6?20 digits; leading zeroes are preserved.')
    if state['account_type'] not in ('current','savings','transmission','other'):errors.append('Choose an account type.')
    if state['account_type']=='other' and not state['account_type_other']:errors.append('Describe the other account type.')
    if state['account_type']!='other' and state['account_type_other']:errors.append('Clear the other account type description unless Other is selected.')
    swift = state['swift']
    if swift and not re.fullmatch(r'[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?',swift):errors.append('SWIFT / BIC must be 8 or 11 uppercase letters/digits in BIC format, or blank where not applicable.')
    return state, errors


def banking_utilities_placeholder_values(items):
    state = banking_utilities_state(items)
    # A stored/observed value is not automatically a confirmed arrangement.
    if not state['utilities_confirmed'] or not state['banking_confirmed']:
        return {token:'' for token in ANCILLARY_TOKENS}
    reference = state['utility_references'] if state['utility_reference_mode']=='provided' else 'No separate references supplied: ' + state['utility_no_references_reason']
    kind = state['account_type_other'] if state['account_type']=='other' else state['account_type'].capitalize()
    banking = ['Bank: ' + state['bank_name'],
               'Branch: ' + (state['branch_name'] + ' / ' if state['branch_name'] else '') + state['branch_code'],
               'Account holder: ' + state['account_holder'], 'Account type: ' + kind,
               'Account number: ' + state['account_number']]
    if state['swift']:banking.append('SWIFT / BIC: ' + state['swift'])
    return dict(zip(ANCILLARY_TOKENS,(state['utility_allocation_method'],reference,'\n'.join(banking))))



def stage5_placeholder_values(items):
    values = {}
    for mapper in (party_placeholder_values,premises_placeholder_values,period_placeholder_values,
                   amounts_placeholder_values,banking_utilities_placeholder_values):
        values.update(mapper(items))
    # Optional execution details stay blank in storage. Render empty writing lines,
    # never invented dates or unresolved tokens, in this explicitly unsigned draft.
    for token in SIGNATURE_TOKENS:
        if not values[token]:values[token] = '________________'
    return values


def stage5_completion(items):
    issues = []
    required = [('_parties','parties','s1-03'),('_premises','premises','s1-10'),
                ('_amounts','amounts','s1-21'),('_banking_utilities','amounts','banking-utilities')]
    for saved,group,anchor in required:
        if not isinstance(items.get(saved),dict):
            issues.append((group,anchor,'Complete the structured ' + group + ' particulars for this master.'))
        elif items[saved].get('master_sha256') != PARTIES_MASTER_SHA256:
            issues.append((group,anchor,'Stored particulars refer to an unverified master version; review and save this section.'))
    parties = parties_state(items)
    form = {'parties_form_version':str(items.get('_parties',{}).get('schema_version',1)), **{role+'_'+key:value for role,fields in PARTY_FIELDS.items() for key,value in parties[role].items()}}
    if items.get('_parties',{}).get('schema_version')==3:
        form['representative_owner_id']=str(items.get('_parties',{}).get('lessor',{}).get('owner_id') or '')
        form['confirm_representative_capacity']='on' if items.get('_parties',{}).get('lessor',{}).get('capacity_confirmed') else ''
    _, messages = parties_from_form(form, items)
    issues.extend(('parties','s1-06',message) for message in messages)
    issues.extend(('parties','s1-06',message) for message in tenant_snapshot_errors(items))
    lessors = items.get('_contracting_lessors', {})
    if not lessors.get('confirmed') or not lessors.get('identities') or any(not owner.get('name') or not owner.get('owner_id') for owner in lessors.get('identities', [])):
        issues.append(('parties','s1-03','Confirm the actual contracting lessors from authorized owner records.'))
    if items.get('_parties',{}).get('schema_version') in (2,3) and any(not owner.get('identity_number') for owner in lessors.get('identities',[])):
        issues.append(('parties','s1-03','Each lessor requires an ID or registration number.'))
    form = {'premises_form_version':'1', **{'premises_'+key:value for key,value in premises_state(items).items()}}
    _, messages = premises_from_form(form)
    issues.extend(('premises','s1-10',message) for message in messages)
    issues.extend(('period','s1-18',message) for message in period_alerts(items,require_complete=True))
    form = {'amounts_form_version':str(items.get('_amounts',{}).get('schema_version',1)), **{'amounts_'+key:value for key,value in amounts_state(items).items()}}
    _, messages = amounts_from_form(form,items)
    issues.extend(('amounts','s1-21',message) for message in messages)
    state = banking_utilities_state(items)
    form = {'part5_form_version':'1', **{'part5_'+key:value for key,value in state.items() if isinstance(value,str)}}
    for key in ('utilities_confirmed','banking_confirmed'):form['part5_'+key]='on' if state[key] else ''
    _, messages = banking_utilities_from_form(form)
    issues.extend(('amounts','banking-utilities',message) for message in messages)
    if items.get('_master', {}).get('sha256', PARTIES_MASTER_SHA256) != PARTIES_MASTER_SHA256:
        issues.append(('parties','s1-03','This draft is pinned to a different master; do not silently replace it.'))
    if items.get('_additional_clauses') or any(items.get('sc-'+str(i).zfill(2),{}).get('value') for i in range(1,11)):
        issues.append(('conditions','additional-clauses','Additional contractual conditions have no merge slot in this master. Resolve their approved incorporation before generation.'))
    if not issues:
        try:
            values=stage5_placeholder_values(items)
            if len(values)!=37 or any(not value.strip() or re.search(r'\[[^\[\]]+\]',value) for value in values.values()):
                issues.append(('parties','s1-03','All 37 master placeholders require explicit resolved values.'))
        except ValueError:issues.append(('amounts','s1-21','Resolve invalid financial values before generation.'))
    return issues
