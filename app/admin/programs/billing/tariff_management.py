"""Administrator tariff pages. Start-only versions have implicit, exclusive next-start ends."""
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from flask import abort, flash, redirect, request, url_for
from flask_login import current_user
import re
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.utils.roles import is_admin
from app.utils.tariff_versions import identity, resolve

TYPES = {'electricity': 'Electricity', 'water': 'Water', 'sewerage': 'Sewerage',
         'water_fixed': 'Water Fixed Charge', 'sewerage_fixed': 'Sewerage Fixed Charge'}


def today():
    return datetime.now(ZoneInfo('Africa/Johannesburg')).date()


def authorised():
    if not current_user.is_authenticated or not is_admin():
        abort(403)


def records():
    return [dict(r) for r in db.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]


def family(tariff_id):
    rows = records()
    source = next((r for r in rows if r['id'] == tariff_id), None)
    if source is None:
        abort(404)
    return source, [r for r in rows if identity(r) == identity(source)]


def listing(render):
    authorised()
    # Historical overwrite/delete submissions are intentionally no longer accepted.
    if request.method != 'GET':
        abort(405)
    groups = {}
    for row in records():
        groups.setdefault(identity(row), []).append(row)
    verification = verification_records()
    tariffs = []
    for versions in groups.values():
        selected, issues = resolve(versions, today())
        source = max(versions, key=lambda r: (r['effective_date'] or '', r['id']))
        current = selected[0] if selected else None
        tariffs.append(dict(source=source, current=current, issues=issues,
                            verification=verification.get((current or source)['id']) if verification is not None else None,
                            verification_available=verification is not None,
                            scheduled=any((r['effective_date'] or '') > today().isoformat() for r in versions)))
    tariffs.sort(key=lambda t: (t['source']['utility_type'], t['source']['block_start'] or 0, t['source']['code']))
    return render('admin/programs/billing/tariffs.html', tariffs=tariffs, types=TYPES)


def form(render, tariff_id=None):
    authorised()
    source, versions = family(tariff_id) if tariff_id is not None else (None, [])
    source = max(versions, key=lambda r: (r['effective_date'] or '', r['id'])) if versions else source
    selected, issues = resolve(versions, today()) if versions else ([], [])
    current = selected[0] if selected else None
    values = dict(request.form) if request.method == 'POST' else {}
    error = None
    status = 200
    ready = verification_ready()
    if not ready:
        error = 'Municipal tariff verification is unavailable. Database setup must be completed before saving.'
    if request.method == 'POST':
        try:
            if not ready:
                raise ValueError(error)
            if source is not None:
                save_rate(tariff_id, values)
            else:
                save_new(values)
            flash('New tariff rate version saved successfully.' if source else 'Tariff saved successfully.', 'success')
            return redirect(url_for('admin_bp.billing_tariffs'))
        except ValueError as exc:
            db.session.rollback()
            error, status = str(exc), 400
        except SQLAlchemyError:
            db.session.rollback()
            error, status = 'The tariff could not be saved. Please retry; no existing rate was changed.', 503
    return render('admin/programs/billing/tariff_form.html', source=source, current=current,
                  values=values, types=TYPES, error=error, issues=issues,
                  code_previews={utility: next_code(utility) for utility in TYPES} if source is None else {},
                  verification_ready=ready, verifier_name=getattr(current_user, 'name', '') or ('Administrator ' + str(current_user.id))), status


def history(render, tariff_id):
    authorised()
    source, versions = family(tariff_id)
    ordered = sorted(versions, key=lambda r: (r['effective_date'] or '', r['id']))
    entries = []
    valid = []
    for row in ordered:
        try:
            start = date.fromisoformat(row['effective_date'])
            if start.isoformat() != row['effective_date']:
                raise ValueError()
        except (TypeError, ValueError):
            start = None
        valid.append((row, start))
    unknown_dates = any(start is None for _, start in valid)
    for i, (row, start) in enumerate(valid):
        next_start = valid[i+1][1] if i+1 < len(valid) else None
        duplicate = start and sum(s == start for _, s in valid) > 1
        status = ('Unknown' if unknown_dates else 'Conflict' if duplicate else
                  'Scheduled' if start > today() else 'Previous' if next_start and next_start <= today() else 'Current')
        end = next_start - timedelta(days=1) if start and next_start and next_start > start else None
        entries.append(dict(row=row, start=start, end=end, status=status))
    verification = verification_records()
    return render('admin/programs/billing/tariff_history.html', source=source, entries=entries,
                  verification=verification or {}, verification_available=verification is not None)


def number(values, key, label, places=4, default=None):
    raw = values.get(key, default)
    try:
        value = Decimal(str(raw))
        if not value.is_finite() or value < 0 or value > Decimal('999999999'):
            raise InvalidOperation()
        if value != value.quantize(Decimal(1).scaleb(-places)):
            raise InvalidOperation()
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(label + ' must be a non-negative number with at most ' + str(places) + ' decimal places.')
    return value


def effective(values):
    raw = values.get('effective_date', '')
    try:
        parsed = date.fromisoformat(raw)
        if parsed.isoformat() != raw:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError('Enter a valid Effective From date.')
    return parsed


def lock_identity(utility, code):
    # All tariff writes pass this transaction-scoped PostgreSQL lock. The second
    # request re-reads committed rows after the first releases the same lock.
    if db.session.execute(text('SHOW transaction_isolation')).scalar() != 'read committed':
        raise ValueError('Tariff saving requires PostgreSQL READ COMMITTED isolation. Please contact the administrator.')
    db.session.execute(text("SET LOCAL lock_timeout = '5s'"))
    db.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                       {'key': 'pm-tariff:' + utility.strip().lower() + ':' + code.strip().casefold()})


def insert_version(values, evidence):
    keys = ('utility_type', 'code', 'description', 'rate', 'block_start', 'block_end',
            'reduction_factor', 'unit', 'effective_date')
    # Only INSERT is permitted: no updates/deletes of historical rows.
    tariff_id = db.session.execute(text('INSERT INTO bil_tariff (' + ','.join(keys) + ') VALUES (' +
                            ','.join(':' + key for key in keys) + ') RETURNING id'), {key: values[key] for key in keys}).scalar_one()
    db.session.execute(text('INSERT INTO bil_tariff_verification (tariff_id,municipality,municipal_category,source_reference,effective_date,verified_by) VALUES (:tariff_id,:municipality,:municipal_category,:source_reference,:effective_date,:verified_by)'),
                       dict(evidence, tariff_id=tariff_id, effective_date=values['effective_date']))
    db.session.commit()


def save_new(values):
    evidence = verification_input(values)
    utility = values.get('utility_type', '')
    if utility not in TYPES:
        raise ValueError('Select a valid Type of Charge.')
    description = values.get('description', '').strip()
    if len(description) > 1000:
        raise ValueError('Description must contain at most 1000 characters.')
    start = effective(values)
    rate = number(values, 'rate', 'Rate')
    tiered = utility in ('water', 'sewerage')
    lower = number(values, 'block_start', 'Consumption From', 2, '0') if tiered else Decimal(0)
    upper = number(values, 'block_end', 'Consumption To', 2, '0') if tiered else Decimal(0)
    factor = number(values, 'reduction_factor', 'Calculation Factor', 3, '1') if tiered else Decimal(1)
    if tiered and (upper <= lower or factor <= 0):
        raise ValueError('Consumption To must exceed Consumption From and Calculation Factor must be positive.')
    unit = values.get('unit', '')
    if unit not in ('', 'kL', 'kWh', 'fixed'):
        raise ValueError('Select a valid unit.')
    if unit and unit != ('kL' if tiered else 'kWh' if utility == 'electricity' else 'fixed'):
        raise ValueError('The selected unit does not match the Type of Charge.')
    # Serialise allocation by type before generating the authoritative code.
    lock_identity(utility, '__code_allocation__')
    code = next_code(utility)
    lock_identity(utility, code)
    if any(identity(r) == (utility, code.casefold()) for r in records()):
        raise ValueError('This tariff identity already exists. Use Edit to save a new rate version.')
    insert_version(dict(utility_type=utility, code=code, description=description, rate=rate,
                        block_start=lower, block_end=upper, reduction_factor=factor,
                        unit=unit or None, effective_date=start.isoformat()), evidence)


def save_rate(tariff_id, values):
    evidence = verification_input(values)
    source, _ = family(tariff_id)
    start = effective(values)
    rate = number(values, 'rate', 'New Rate')
    description = values.get('description', '').strip()
    if len(description) > 1000:
        raise ValueError('Description must contain at most 1000 characters.')
    # Backdating a price version could alter recalculation of an already billed
    # period. Historical corrections need a separately authorised workflow.
    if start < today():
        raise ValueError('Effective From cannot be in the past. Historical prices must remain unchanged.')
    lock_identity(source['utility_type'], source['code'])
    _, versions = family(tariff_id)
    _, issues = resolve(versions, date.max)
    if issues:
        raise ValueError(' '.join(issues))
    latest = max(versions, key=lambda r: r['effective_date'])
    if str(values.get('base_id', '')) != str(latest['id']):
        raise ValueError('A new tariff version was saved while this form was open. Reopen Edit to review it; your entered values are retained below.')
    if start.isoformat() <= latest['effective_date']:
        raise ValueError('Effective From must be later than the latest stored version (' + latest['effective_date'] + '). Duplicate or overlapping starts are not permitted.')
    replacement = dict(latest)
    replacement.update(rate=rate, description=description, effective_date=start.isoformat())
    # Utility, code, units, boundaries and even NULL/999 factors are copied from
    # the locked database row, never accepted from hidden/browser fields.
    insert_version(replacement, evidence)


CODE_PREFIXES = {'electricity': 'ELEC', 'water': 'W', 'sewerage': 'S',
                 'water_fixed': 'WF', 'sewerage_fixed': 'SF'}


def next_code(utility):
    """Preview only unless called under the type-allocation transaction lock.

    Historical versions share a code and do not consume additional numbers.
    Existing named fixed charges are retained, not renamed or reinterpreted.
    """
    prefix = CODE_PREFIXES[utility]
    pattern = re.compile(re.escape(prefix) + r'(\d+)', re.IGNORECASE)
    numbers = []
    for row in records():
        if identity(row)[0] == utility:
            match = pattern.fullmatch(str(row['code']).strip())
            if match:
                numbers.append(int(match.group(1)))
    return prefix + str(max(numbers, default=0) + 1).zfill(2)


def verification_ready():
    required = {'tariff_id','municipality','municipal_category','source_reference','effective_date',
                'verified_by','verified_at','recorded_at'}
    columns = set(db.session.execute(text("SELECT attname FROM pg_attribute WHERE attrelid=to_regclass('bil_tariff_verification') AND attnum > 0 AND NOT attisdropped")).scalars())
    return required <= columns


def verification_records():
    if not verification_ready():
        return None
    return {r['tariff_id']: dict(r) for r in db.session.execute(text('SELECT * FROM bil_tariff_verification')).mappings()}


def verification_input(values):
    authorised()
    if not verification_ready():
        raise ValueError('Municipal tariff verification is unavailable. Database setup must be completed before saving.')
    evidence = {}
    for key, label, maximum in (('municipality','Municipality',200),
                                ('municipal_category','Municipal tariff category',200),
                                ('source_reference','Official source or municipal bill reference',1000)):
        value = values.get(key, '').strip()
        if not value or len(value) > maximum:
            raise ValueError(label + ' is required and must contain at most ' + str(maximum) + ' characters.')
        evidence[key] = value
    # The actor and timestamps are authoritative server/database values, never form inputs.
    evidence['verified_by'] = int(current_user.id)
    return evidence
