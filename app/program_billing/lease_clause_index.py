"""Derived Full Lease clause indexes in existing draft metadata; no contract edits.

The input fingerprint excludes working Addendum selections and audit/version counters.
A PostgreSQL row lock serialises first builds across workers. Only the derived cache
path is updated, without changing the contractual draft version. PDF bytes are a
bounded, private in-process acceleration; the durable index survives worker restarts.
"""
from collections import OrderedDict
from hashlib import sha256
from pathlib import Path
from datetime import datetime, timezone
import json
from flask import abort, current_app
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db

SCHEMA = 1


def inputs(prop, draft):
    from .lease_kit import preview_context
    context = preview_context(prop['id'])
    if str(context['lease_draft']['id']) != str(draft['id']):
        abort(404)
    # Rendered values include live register identities as well as saved particulars.
    ignored = {'lease_draft', 'components', 'pdf_url', 'pdf_email_url', 'filed_sc_pdf_url',
        'core_pdf_url', 'capture_url', 'back_url', 'selected_component', 'selected_label',
        'selected_documents', 'pdf_view', 'print_available', 'document_token'}
    values = {key: value for key, value in context.items() if key not in ignored and not callable(value)}
    values['items'] = {key: value for key, value in context['lease_draft']['items'].items()
        if key != '_lease_history'}
    # Renderer/template changes invalidate the index even without a particulars edit.
    directory = Path(__file__).parent
    templates = Path(current_app.template_folder) / 'program_billing'
    if not templates.is_absolute():
        templates = Path(current_app.root_path) / templates
    files = list(templates.glob('lease*.html')) + list(directory.glob('lease_*.py'))
    files += [Path(current_app.static_folder) / 'css' / 'lease_kit.css']
    revision = [(path.name, sha256(path.read_bytes()).hexdigest()) for path in sorted(files) if path.is_file()]
    version = sha256(json.dumps({'schema': SCHEMA, 'values': values, 'renderer': revision},
        sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()
    return context, version


def cached_pdf(key, payload=None):
    cache = current_app.extensions.setdefault('lease_clause_pdf_cache', OrderedDict())
    if payload is not None:
        cache[key] = payload
        cache.move_to_end(key)
        while len(cache) > 3:
            cache.popitem(last=False)
    return cache.get(key)


def load(prop, draft, extractor, *, with_pdf=False):
    from .lease_kit_pdf import build_pdf
    from werkzeug.exceptions import HTTPException
    try:
        context, version = inputs(prop, draft)
        cache_key = (str(current_user.id), str(prop['id']), str(draft['id']), version)
        saved = draft['items'].get('_lease_history', {}).get('full_lease_clause_index', {})
        if saved.get('input_version') == version and saved.get('schema') == SCHEMA:
            if not with_pdf:
                return saved, None
            payload = cached_pdf(cache_key)
            if payload is not None:
                return saved, payload
        # All workers share this lock/cache, and no source or version is overwritten.
        row = db.session.execute(text("""SELECT items,version FROM bil_lease_draft
            WHERE id=:d AND property_id=:p AND manager_id=:u FOR UPDATE"""),
            {'d': draft['id'], 'p': prop['id'], 'u': current_user.id}).mappings().first()
        if row is None:
            abort(404)
        if row['version'] != draft['version']:
            abort(409, description='The lease changed while loading its Clause Index. Reopen Select Clause.')
        saved = row['items'].get('_lease_history', {}).get('full_lease_clause_index', {})
        valid = saved.get('input_version') == version and saved.get('schema') == SCHEMA
        payload = cached_pdf(cache_key)
        if not valid or (with_pdf and payload is None):
            payload = build_pdf('lease', context)
            digest = sha256(payload).hexdigest()
            changed = saved.get('source_sha256') != digest or saved.get('schema') != SCHEMA
            if changed:
                units, notices = extractor(payload)
                units = [unit for unit in units if unit['source'] != 'full_lease']
                for unit in units:
                    unit['source_version'] = digest
                    # A title/identifying line must come from the PDF, never a template guess.
                    lines = [line.strip() for line in unit['original'].splitlines() if line.strip()]
                    unit['title'] = ' — '.join(lines[:2]) if lines and lines[0] == unit['reference'] else (lines[0] if lines else '')
                saved = dict(schema=SCHEMA, input_version=version, source_sha256=digest,
                    entries=units, notices=notices, built_at=datetime.now(timezone.utc).isoformat())
            if not valid or changed:
                saved = dict(saved, input_version=version)
                db.session.execute(text("""UPDATE bil_lease_draft SET items=jsonb_set(items,
                    '{_lease_history}', COALESCE(items->'_lease_history','{}'::jsonb) ||
                    jsonb_build_object('full_lease_clause_index',CAST(:index AS jsonb)))
                    WHERE id=:d AND property_id=:p AND manager_id=:u AND version=:v"""),
                    {'index': json.dumps(saved), 'd': draft['id'], 'p': prop['id'],
                     'u': current_user.id, 'v': draft['version']})
            cached_pdf(cache_key, payload)
        db.session.commit()
        return saved, payload if with_pdf else None
    except HTTPException:
        db.session.rollback()
        raise
    except Exception as error:
        db.session.rollback()
        current_app.logger.warning('Addendum Clause Index unavailable error=%s', type(error).__name__)
        abort(503, description='The Full Lease Clause Index could not be generated. No older SC records or invented entries have been substituted. Please try again; if the problem persists, contact support.')
