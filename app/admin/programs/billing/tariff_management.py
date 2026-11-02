"""Administrator tariff pages. Start-only versions have implicit, exclusive next-start ends."""
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from flask import abort, flash, redirect, request, url_for
from flask_login import current_user
from sqlalchemy import text
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
    tariffs = []
    for versions in groups.values():
        selected, issues = resolve(versions, today())
        source = max(versions, key=lambda r: (r['effective_date'] or '', r['id']))
        current = selected[0] if selected else None
        tariffs.append(dict(source=source, current=current, issues=issues,
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
    if request.method == 'POST':
        abort(405)
    return render('admin/programs/billing/tariff_form.html', source=source, current=current,
                  values=values, types=TYPES, error=None, issues=issues)


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
    for i, (row, start) in enumerate(valid):
        next_start = valid[i+1][1] if i+1 < len(valid) else None
        duplicate = start and sum(s == start for _, s in valid) > 1
        status = ('Unknown' if start is None else 'Conflict' if duplicate else
                  'Scheduled' if start > today() else 'Previous' if next_start and next_start <= today() else 'Current')
        end = next_start - timedelta(days=1) if start and next_start and next_start > start else None
        entries.append(dict(row=row, start=start, end=end, status=status))
    return render('admin/programs/billing/tariff_history.html', source=source, entries=entries)
