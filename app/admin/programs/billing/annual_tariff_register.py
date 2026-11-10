"""Read-only annual review of existing versions against unapproved candidates."""
import csv
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from flask import abort, request
from app.utils.tariff_versions import identity, resolve

YEARS = ('2024/25', '2025/26', '2026/27')
CANDIDATES = Path(__file__).resolve().parents[3] / 'static/data/pm_annual_tariff_candidates.csv'


def candidates(year):
    with CANDIDATES.open(encoding='utf-8-sig', newline='') as source:
        return [dict(row, component=row['Existing AIT component'].split(' candidate')[0])
                for row in csv.DictReader(source) if row['Financial year'] == year]


def token(value):
    return re.sub(r'[^a-z0-9]', '', str(value or '').casefold())


def component(row, evidence):
    """Review mapping only; selection and all pricing remain in existing services."""
    code = token(row.get('code'))
    utility = token(row.get('utility_type'))
    category = token(evidence.get('municipal_category'))
    if re.fullmatch(r'w0[1-5]', code) and utility == 'water':
        return code.upper()
    if re.fullmatch(r's0[1-5]', code) and utility in ('sewerage', 'sanitation'):
        return code.upper()
    if utility in ('electricity', 'electrical'):
        return 'ELEC01'
    if code in ('wloss', 'waterlosslevy') or (utility == 'waterfixed' and category in (
            'waterloss', 'waterlosslevy', 'waterlossinsurance', 'waterlosslevyinsurance', 'waterlosscharge')):
        return 'W_LOSS'
    if code == 'wmgt' or (utility == 'waterfixed' and category in (
            'monthlymanagementfee', 'managementfee', 'watermanagementfee')):
        return 'W_MGT'
    if code == 'srefuse' or (utility in ('seweragefixed', 'sanitationfixed') and category in (
            'refuse', 'refusebins', 'refusecharge')):
        return 'S_REFUSE'
    if utility == 'watersurcharge' or category == 'waterinfrastructuresurcharge':
        return 'W_SURCHARGE'
    if utility == 'seweragesurcharge' or category in (
            'sewerageinfrastructuresurcharge', 'sewagedisposalinfrastructuresurcharge'):
        return 'S_SURCHARGE'
    if 'surcharge' in code:
        if utility.startswith('water') or code.startswith('w'):
            return 'W_SURCHARGE'
        if utility in ('sewerage', 'sanitation', 'seweragefixed') or code.startswith('s'):
            return 'S_SURCHARGE'
    return None


def decimal(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() and result >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def comparable_unit(code, unit, row):
    unit = str(unit or '').casefold().replace(' ', '')
    if code == 'ELEC01':
        return unit in ('kwh', 'r/kwh', 'zar/kwh')
    if code == 'W_LOSS':
        return unit in ('fixed', 'r/month', 'zar/month')
    if code.endswith('_SURCHARGE'):
        return unit in ('r/kl', 'zar/kl', 'perkl') or (
            code == 'S_SURCHARGE' and unit == 'kl'
            and str(row.get('code') or '').strip().casefold() == 's_surcharge'
            and decimal(row.get('reduction_factor')) == 1)
    return unit in ('kl', 'r/kl', 'zar/kl', 'perkl', 'kl/day')


def category_matches(code, category):
    category = token(category)
    if re.fullmatch(r'[WS]0[1-5]', code):
        return category.startswith('domestic')
    if code == 'ELEC01':
        return category.startswith('residential')
    return category in {
        'waterloss', 'waterlosslevy', 'waterlossinsurance', 'waterlosslevyinsurance', 'waterlosscharge',
        'waterinfrastructuresurcharge', 'sewerageinfrastructuresurcharge',
        'sewagedisposalinfrastructuresurcharge'}


def review(stored, verification, year):
    start = date(int(year[:4]), 7, 1)
    end = date(start.year + 1, 6, 30)
    evidence = verification or {}
    groups = {}
    unmapped = []
    for row in stored:
        key = component(row, evidence.get(row['id'], {}))
        if key:
            groups.setdefault(key, []).append(dict(row))
        else:
            unmapped.append(dict(row))
    results = []
    for candidate in candidates(year):
        code = candidate['component']
        versions = groups.get(code, [])
        # Component aliases share one review family. Multiple electricity codes
        # are ambiguous: never choose the latest of different electricity scales.
        selected, issues = resolve(versions, end, key_function=lambda row: (code, code))
        if code == 'ELEC01' and len({identity(row)[1] for row in versions if str(row.get('effective_date')) <= end.isoformat()}) > 1:
            selected = []
            issues.append('Multiple electricity identities: no tariff scale was guessed.')
        existing = selected[0] if selected and not issues else None
        annual = any(str(row.get('effective_date')) == start.isoformat() for row in versions)
        verification_row = evidence.get(existing['id'], {}) if existing else {}
        provenance = str(verification_row.get('source_reference') or '')
        estimated = bool(existing) and (
            provenance.startswith('AIT_CURRENT_RATE_SUBSTITUTE_V1:') or
            'CURRENT-RATE SUBSTITUTE' in str(existing.get('description') or '').upper())
        if provenance.startswith('AIT_ANNUAL_TARIFF_ALIGNMENT_V1:'):
            try:
                estimated = estimated or json.loads(provenance.split(':', 1)[1]).get('estimated') is True
            except (ValueError, AttributeError):
                issues.append('Annual provenance requires review; no verification was inferred.')
        hold = candidate['Review status'].startswith('HOLD')
        proposed = decimal(candidate['Candidate municipal rate - NOT approved'])
        rate = decimal(existing.get('rate')) if existing else None
        same_basis = bool(existing) and code not in ('W_MGT', 'S_REFUSE') and comparable_unit(code, existing.get('unit'), existing)
        municipality = token(verification_row.get('municipality'))
        if municipality and municipality not in ('ethekwini', 'ethekwinimunicipality', 'ethekwinimetropolitanmunicipality'):
            same_basis = False
            issues.append('Stored municipality does not match the eThekwini candidate.')
        difference = proposed - rate if same_basis and proposed is not None and rate is not None else None
        status = 'Pending review'
        if not existing:
            status = 'Conflicting' if issues else 'Missing'
        elif rate is None or issues:
            status = 'Conflicting'
            if rate is None:
                issues.append('Stored rate is invalid; no comparison was calculated.')
        elif hold and 'conflict' in candidate['Review status'].lower():
            status = 'Conflicting'
        elif (not hold and not estimated and not provenance.startswith('AIT_') and annual and difference == 0
              and category_matches(code, verification_row.get('municipal_category'))
              and all(verification_row.get(key) for key in (
                  'municipality', 'municipal_category', 'source_reference', 'verified_by', 'verified_at'))
              and candidate['Source / PDF page'] in provenance):
            status = 'Verified'
        carry = bool(existing) and str(existing['effective_date']) < start.isoformat()
        if not annual:
            coverage = 'Missing 1 July version. ' + (
                'Older version carried forward.' if carry else
                'Within-year version only; review opening coverage.' if existing else
                'No applicable unambiguous version at financial-year end.')
        else:
            coverage = '1 July version recorded; latest applicable stored version shown below.'
        results.append(dict(candidate=candidate, component=code, existing=existing,
                            versions=sorted(versions, key=lambda row: (str(row.get('effective_date')), row['id'])),
                            verification=verification_row, status=status, issues=issues,
                            annual=annual, carry=carry, coverage=coverage, estimated=estimated,
                            proposed=proposed, rate=rate, difference=difference, hold=hold))
    return dict(rows=results, unmapped=unmapped, start=start, end=end,
                verification_available=verification is not None)


def page(render):
    from app.admin.programs.billing.tariff_management import authorised, records, verification_records, today
    authorised()
    if request.method != 'GET':
        abort(405)
    year = request.args.get('financial_year', YEARS[-1])
    if year not in YEARS:
        abort(400, description='Select a supported financial year.')
    opened = request.args.get('workspace') == 'register'
    data = {}
    if opened:
        stored, evidence = records(), verification_records()
        data = review(stored, evidence, year)
        from app.admin.programs.billing.annual_tariff_activation import options
        data['activation_options'] = options(stored, evidence, year, today())
    from app.admin.programs.billing.municipal_tariff_monitor import summary
    data['monitoring_checks']=summary()
    from app.admin.programs.billing.municipal_tariff_overview import build
    data['overview']=build(year)
    return render('admin/programs/billing/annual_tariff_register.html', years=YEARS,
                  financial_year=year, opened=opened, **data)
