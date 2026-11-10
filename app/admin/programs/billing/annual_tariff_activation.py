"""Explicit single-component approval using existing insert-only tariff auditing."""
import hashlib
import json
import re
from importlib import import_module
from datetime import date, timedelta
from decimal import Decimal

from flask import abort, current_app, flash, redirect, request, url_for
from flask_login import current_user
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy.exc import SQLAlchemyError

from app.utils.tariff_versions import identity, resolve
from app.admin.programs.billing.annual_tariff_register import candidates, component, decimal, token

START = date(2026, 7, 1)
YEAR = '2026/27'
SOURCE = 'https://www.durban.gov.za/uploads/0000/13/2026/06/22/eth-final-2026-2027-tariff-tables.pdf'
SOURCE_SHA256 = '59b640ec20d9a8a408ebd92a378dae66632470e703a7f3fed2509a5bf9c6a56e'
LOWERS = (0, 201, 834, 1001, 1501)
UPPERS = (200, 833, 1000, 1500, None)
WIDTHS = (Decimal('.2'), Decimal('.633'), Decimal('.167'), Decimal('.5'), Decimal(999))
FRACTIONS = ('.95', '.75', '.75', '.65', '.60')


def plan(stored, evidence, year, code, today):
    if year != YEAR or not re.fullmatch(r'[SW]0[1-5]', code):
        raise ValueError('This component/year is on HOLD or outside the verified Stage 2 scope.')
    if evidence is None:
        raise ValueError('Tariff verification storage is unavailable; activation is disabled.')
    matches = [row for row in candidates(year) if row['component'] == code]
    if len(matches) != 1:
        raise ValueError('A unique candidate source is required.')
    candidate = matches[0]
    index = int(code[-1]) - 1
    source_page = '284' if code.startswith('S') else '268'
    if (candidate['Review status'].startswith('HOLD') or not candidate.get('Source verified')
            or candidate.get('Source SHA256') != SOURCE_SHA256
            or candidate.get('Source / PDF page') != SOURCE+'#page='+source_page
            or candidate.get('Financial year start') != START.isoformat()
            or candidate.get('Rate unit') != 'R/kL excluding VAT'
            or not candidate.get('Applicable category') or not candidate.get('Municipal band')
            or decimal(candidate.get('Candidate municipal rate - NOT approved')) is None):
        raise ValueError('Verified source, category, units and effective-date evidence are required.')
    if code.startswith('S') and decimal(candidate.get('Discharge fraction')) != Decimal(FRACTIONS[index]):
        raise ValueError('Verified domestic discharge evidence is missing or conflicting.')
    versions = [dict(row) for row in stored if component(row, evidence.get(row['id'], {})) == code]
    _, issues = resolve(versions, date.max)
    if issues:
        raise ValueError(' '.join(issues))
    if len({identity(row) for row in versions}) > 1:
        raise ValueError('Conflicting stored tariff identities require review before activation.')
    if any(str(row['effective_date']) == START.isoformat() for row in versions):
        raise ValueError('A version already starts on 2026-07-01; existing rates will not be overwritten.')
    before, _ = resolve(versions, START-timedelta(days=1))
    active, _ = resolve(versions, today)
    previous = before[0] if before else None
    source = previous or (min(versions, key=lambda row: row['effective_date']) if versions else None)
    utility = source['utility_type'] if source else ('sewerage' if code.startswith('S') else 'water')
    if source:
        unit = str(source.get('unit') or '').casefold().replace(' ', '')
        if unit not in ('kl', 'r/kl', 'zar/kl'):
            raise ValueError('Stored rate units need verification; activation cannot rename or infer them.')
        if decimal(source.get('block_start')) != LOWERS[index]:
            raise ValueError('Stored band threshold conflicts with the verified component.')
        factor = decimal(source.get('reduction_factor'))
        if factor is None or factor <= 0:
            raise ValueError('Stored allocation metadata is invalid; no calculation factor was changed.')
        if source.get('block_end') is not None and (decimal(source['block_end']) is None or decimal(source['block_end']) < LOWERS[index]):
            raise ValueError('Stored upper band threshold is invalid.')
        if code.startswith('W') and (factor != WIDTHS[index] or (
                index < 4 and decimal(source.get('block_end')) != UPPERS[index])):
            raise ValueError('Stored water band widths require review; no calculation metadata was changed.')
        if code.startswith('W') and index == 4 and source.get('block_end') is not None and decimal(source['block_end']) < LOWERS[index]:
            raise ValueError('Stored final water band is invalid.')
        prior_evidence = evidence.get(source['id'], {})
        municipality = token(prior_evidence.get('municipality'))
        category = token(prior_evidence.get('municipal_category'))
        if municipality and municipality not in ('ethekwini', 'ethekwinimunicipality', 'ethekwinimetropolitanmunicipality'):
            raise ValueError('Stored municipality conflicts with this candidate.')
        if category and not category.startswith('domestic'):
            raise ValueError('Stored category is not the verified domestic category; review it first.')
        if ('tank' in category and not any(label in category for label in ('noindividual', 'withoutindividual', 'directsupply'))
                or any(label in category for label in ('exceedstwostoreys', 'morethantwostoreys', 'threestoreys'))):
            raise ValueError('Stored tank/development category conflicts with the verified applicability.')
    # Keep all dimensions and identifiers on existing families, including the
    # legacy S04 factor 999. Corrected sewerage widths/fractions are independent.
    new = dict(source) if source else dict(utility_type=utility, code=code,
            description=('Sewerage' if code.startswith('S') else 'Water')+' '+candidate['Municipal band'],
            block_start=LOWERS[index], block_end=UPPERS[index], reduction_factor=WIDTHS[index], unit='kL')
    new.update(rate=Decimal(candidate['Candidate municipal rate - NOT approved']), effective_date=START.isoformat())
    new['description'] = re.sub(r'^\s*CURRENT-RATE SUBSTITUTE FOR \d{4}/(?:\d{4}|\d{2})\s*-\s*', '', new.get('description') or '', flags=re.I)
    future = [date.fromisoformat(str(row['effective_date'])) for row in versions if str(row['effective_date']) > START.isoformat()]
    end = min(future)-timedelta(days=1) if future else None
    snapshot = dict(candidate=candidate, review_date=today.isoformat(), versions=sorted(versions,key=lambda row:row['id']),
                    evidence={str(row['id']):evidence.get(row['id']) for row in versions})
    digest = hashlib.sha256(json.dumps(snapshot,sort_keys=True,default=str).encode()).hexdigest()
    return dict(candidate=candidate, code=code, year=year, new=new, previous=previous,
                active=active[0] if active else None, end=end, versions=versions,
                digest=digest, lock=(utility,new['code']))


def options(stored, evidence, year, today):
    result = {}
    for side in ('S', 'W'):
        for index in range(1,6):
            code=side+str(index).zfill(2)
            try:
                plan(stored,evidence,year,code,today)
                result[code]=dict(ready=True)
            except ValueError as exc:
                result[code]=dict(ready=False,reason=str(exc))
    return result


def serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt='pm-annual-tariff-approval-v1')


def confirmation_token(prepared):
    return serializer().dumps(dict(actor=int(current_user.id), year=prepared['year'],
                                   code=prepared['code'], digest=prepared['digest']))


def activate(service, year, code, signed, confirmed):
    service.authorised()
    if confirmed != 'yes':
        raise ValueError('Explicit confirmation of the source and domestic applicability is required.')
    try:
        approved=serializer().loads(signed, max_age=900)
    except (BadSignature, SignatureExpired, TypeError):
        raise ValueError('Approval expired or could not be verified. Review this component again.')
    if approved.get('actor') != int(current_user.id):
        abort(403)
    prepared=plan(service.records(),service.verification_records(),year,code,service.today())
    service.lock_identity(*prepared['lock'])
    # Re-read under the SAME family lock used by ordinary tariff administration.
    prepared=plan(service.records(),service.verification_records(),year,code,service.today())
    if approved != dict(actor=int(current_user.id),year=year,code=code,digest=prepared['digest']):
        raise ValueError('Stored versions or candidate evidence changed. Reopen the confirmation before saving.')
    candidate=prepared['candidate']
    reference=(candidate['Source / PDF page']+' | Annual register approval '+year+'; '+code+
               '; PDF SHA256 '+candidate['Source SHA256']+
               '; band '+candidate['Municipal band']+
               ('; discharge '+candidate['Discharge fraction']+' (official PDF page287)' if code.startswith('S') else '')+
               '; supersedes '+str(prepared['previous']['id'] if prepared['previous'] else 'no earlier version')+
               '; confirmed domestic applicability; snapshot '+prepared['digest'])
    evidence=service.verification_input(dict(municipality='eThekwini Municipality',
            municipal_category=candidate['Applicable category'], source_reference=reference))
    service.insert_version(prepared['new'],evidence)


def page(render, code):
    service = import_module('app.admin.programs.billing.tariff_management')
    service.authorised()
    year=request.values.get('financial_year',YEAR)
    error=None;status=200;prepared=None
    try:
        if request.method == 'POST':
            activate(service,year,code,request.form.get('approval_token'),request.form.get('confirmed'))
            flash(code+' annual tariff approved and activated from 2026-07-01. Historical versions were preserved.','success')
            return redirect(url_for('admin_bp.billing_tariff_annual',financial_year=year,workspace='register'))
        prepared=plan(service.records(),service.verification_records(),year,code,service.today())
    except ValueError as exc:
        service.db.session.rollback();error=str(exc);status=409 if request.method=='POST' else 400
    except SQLAlchemyError:
        service.db.session.rollback();current_app.logger.exception('Annual tariff activation failed')
        error='Activation failed. No tariff change was committed. Review this component before retrying.';status=503
    return render('admin/programs/billing/annual_tariff_confirm.html',financial_year=year,
                  prepared=prepared,error=error,approval_token=confirmation_token(prepared) if prepared else None),status
