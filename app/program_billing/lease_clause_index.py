"""Persistent heading-only indexes. Never render or extract a PDF here."""
from hashlib import sha256
from pathlib import Path
from datetime import datetime, timezone
import json
from flask import abort, current_app
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db

SCHEMA = 3


def inputs(prop, draft):
    from .lease_core_master import pinned, headings
    core = headings(pinned(draft))
    items = {key: value for key, value in draft['items'].items() if key != '_lease_history'}
    directory = Path(__file__).parent
    revision = [(name, sha256((directory / name).read_bytes()).hexdigest())
        for name in ('lease_certificate_form.py', 'lease_certificate_layout.json', 'lease_signing.py', 'lease_clause_index.py', 'lease_kit.py', 'lease_kit_pdf.py')]
    templates=Path(current_app.root_path).parent/'templates'/'program_billing'
    revision.extend((path.name,sha256(path.read_bytes()).hexdigest()) for path in sorted(templates.glob('lease_*document.html')))
    value = dict(property_name=prop.get('name'),property_address=prop.get('address'),schema=SCHEMA, property_id=prop['id'], draft_id=str(draft['id']),
        items=items, core_headings=core, renderer=revision)
    version = sha256(json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()
    return core, version


def existing(prop, draft):
    _, version = inputs(prop, draft)
    saved = draft['items'].get('_lease_history', {}).get('full_lease_clause_index', {})
    if saved.get('input_version') != version or saved.get('schema') != SCHEMA:
        abort(409, description='The saved Clause Heading Index is unavailable or out of date. Use Retry Index. Saved selections are unchanged.')
    return saved


def entries(items, core):
    from . import lease_certificate_form as form
    from .lease_supplementary_conditions import legacy_entries
    result = []
    seen = set()
    labels = dict(certificate='Certificate', core='Core Lease',
        special_conditions='Special Conditions', signature='Signatures')
    def add(source, reference, title, provenance):
        reference = str(reference or '').strip()
        title = str(title or '').strip()
        if not reference or (source, reference) in seen:
            raise ValueError('Missing or duplicate clause heading reference: '+reference)
        seen.add((source, reference))
        result.append(dict(id=source+':'+sha256(reference.encode()).hexdigest()[:24],
            source=source, source_label=labels[source], reference=reference, title=title,
            provenance=provenance))
    for section in form.certificate_layout(*form.counts(items)):
        for label, _ in section.get('rows', []):
            add('certificate', label, label, 'Certificate template field')
    if not core['entries']:raise ValueError('Maintain the Core Master heading list before retrying.')
    for row in core['entries']:
        add('core', row['reference'], row['title'], 'Core Master version '+str(core['core_version'])+'; heading revision '+str(core.get('revision', 0)))
    sc=items.get('_supplementary_conditions', {})
    clauses=sc.get('clauses', []) if sc.get('mode')!='none' or items.get('_sc_pdf_current') else []
    for number, row in enumerate(clauses,1):
        add('special_conditions', row.get('number') or 'SC-%02d'%number, row.get('heading'), 'Data Capture heading; not verified against an uploaded SC PDF')
    for row in legacy_entries(items):
        add('special_conditions', row['source'], row.get('label', ''), 'Legacy Data Capture heading; not verified against PDF')
    for role in ('Lessor', 'Lessee'):
        for label in ('Signing place', 'Signing date', 'Printed name', 'Signature', 'Capacity', 'Witness 1', 'Witness 2'):
            add('signature', role+' — '+label, label, 'Signature template field')
    return result


def load(prop, draft, extractor=None, *, with_pdf=False, prepared=None, actor_id=None):
    """Cheap synchronous metadata build; one persisted index per input version."""
    from werkzeug.exceptions import HTTPException
    try:
        core, version = inputs(prop, draft)
        saved = draft['items'].get('_lease_history', {}).get('full_lease_clause_index', {})
        if saved.get('schema') == SCHEMA and saved.get('input_version') == version:
            return saved, None
        notices = ['Heading references only; no original contractual wording is stored in this index.']
        if draft['items'].get('_sc_pdf_current'):
            notices.append('SC headings come from Data Capture and are not verified against the current uploaded PDF. Correct headings in Special Conditions if necessary.')
        saved = dict(schema=SCHEMA, input_version=version, source_sha256=version,
            source_version=version, entries=entries(draft['items'], core), notices=notices,
            built_at=datetime.now(timezone.utc).isoformat())
        for entry in saved['entries']:entry['source_version']=version
        changed = db.session.execute(text("""UPDATE bil_lease_draft SET items=jsonb_set(items,
            '{_lease_history}', COALESCE(items->'_lease_history','{}'::jsonb) ||
            jsonb_build_object('full_lease_clause_index',CAST(:index AS jsonb)))
            WHERE id=:d AND property_id=:p AND manager_id=:u AND version=:v"""),
            dict(index=json.dumps(saved),d=draft['id'],p=prop['id'],
                u=actor_id if actor_id is not None else current_user.id,v=draft['version']))
        if changed.rowcount != 1:
            abort(409,description='The lease changed. Retry Index using the current draft.')
        db.session.commit()
        draft['items'].setdefault('_lease_history',{})['full_lease_clause_index']=saved
        return saved, None
    except HTTPException:
        db.session.rollback()
        raise
    except Exception:
        db.session.rollback()
        current_app.logger.exception('Clause Heading Index unavailable property=%s draft=%s',prop['id'],draft['id'])
        abort(503,description='The Clause Heading Index is unavailable. Check Core Master headings and Data Capture references, then use Retry Index. No PDF extraction or substitute wording was used.')


def assembled(context):
    """Persist heading metadata on Full Lease output without blocking document access."""
    if not context.get('lease_draft'):return
    try:load(context['property'],context['lease_draft'])
    except Exception:
        db.session.rollback()
        current_app.logger.exception('Full Lease assembled; its heading index is unavailable. Manual Retry Index remains available.')
