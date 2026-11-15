"""Prospective Admin-approved ProTrade tariffs using the universal tariff table."""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from flask import abort, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from app.extensions import db
from app.models.billing import TokenTariff
from app.models.mechanic import ProTradePricingRevision
from app.utils.roles import is_admin

DEFAULTS = dict(tokens=dict(quote=10, invoice=10, soa=10, boq_section=25),
    suggested=dict(quote_electronic=1000, quote_printed=1500, soa_electronic=1000, soa_printed=1500),
    boq=dict(contractor_fee_cents=5000, operator_retains_cents=2500))
LABELS = dict(quote='Quotation', invoice='Invoice', soa='Statement of Account', boq_section='BOQ section')
SUGGESTIONS = dict(quote_electronic='Electronic quotation / invoice', quote_printed='Printed quotation / invoice',
    soa_electronic='Electronic SOA', soa_printed='Printed SOA')


def current_prices():
    revision = ProTradePricingRevision.query.order_by(ProTradePricingRevision.id.desc()).first()
    values = deepcopy(revision.values if revision else DEFAULTS)
    for tariff in TokenTariff.query.filter_by(program_slug='protrade').all():
        if tariff.action_name in LABELS:
            values['tokens'][tariff.action_name] = tariff.base_token_cost
    return values


def token_cost(kind, lock=False):
    if kind not in LABELS:
        abort(400)
    query = TokenTariff.query.filter_by(program_slug='protrade', action_name=kind)
    tariff = (query.populate_existing().with_for_update() if lock else query).first()
    return tariff.base_token_cost if tariff else DEFAULTS['tokens'][kind]


def commercial_terms():
    values = current_prices()
    return dict(currency='ZAR', **values['boq'], ait_wallet_value_cents=values['tokens']['boq_section']*100,
        tokens=values['tokens']['boq_section'], contractor_payment_recipient='operator', ait_collection='prepaid_wallet_deduction')


def cents(value):
    amount = Decimal(value)
    if not amount.is_finite() or amount < 0 or amount > 1000000 or amount != amount.quantize(Decimal('.01')):
        raise ValueError('Enter non-negative Rand prices with at most two decimal places.')
    return int(amount*100)


def pricing_page():
    if not is_admin(current_user):
        abort(403)
    error = None
    status = 200
    if request.method == 'POST':
        # Fixed lock order serializes approvals without touching user wallets.
        tariffs = TokenTariff.query.filter_by(program_slug='protrade').order_by(TokenTariff.id).populate_existing().with_for_update().all()
        latest = ProTradePricingRevision.query.order_by(ProTradePricingRevision.id.desc()).first()
        if request.form.get('revision') != str(latest.id if latest else 0):
            abort(409, description='Prices changed. Reopen this page before approving.')
        try:
            if {tariff.action_name for tariff in tariffs} != set(LABELS):
                raise ValueError('Apply the ProTrade pricing migration before approving prices.')
            values = dict(tokens={}, suggested={}, boq={})
            for kind in LABELS:
                cost = int(request.form.get('tokens_'+kind, ''))
                if not 1 <= cost <= 1000000:
                    raise ValueError('Token charges must be whole numbers from 1 to 1000000.')
                values['tokens'][kind] = cost
            for key in SUGGESTIONS:
                values['suggested'][key] = cents(request.form.get('suggested_'+key, ''))
            for key in DEFAULTS['boq']:
                values['boq'][key] = cents(request.form.get(key, ''))
            old = current_prices()
            if values['boq'] != old['boq'] or values['tokens']['boq_section'] != old['tokens']['boq_section']:
                if request.form.get('approve_boq_terms') != 'yes':
                    raise ValueError('Explicitly approve the complete BOQ commercial arrangement.')
            if values['boq']['contractor_fee_cents'] != values['boq']['operator_retains_cents'] + values['tokens']['boq_section']*100:
                raise ValueError('BOQ contractor fee must equal operator earnings plus the AIT token deduction at R1 per token.')
            reason = request.form.get('reason','').strip()
            if not 4 <= len(reason) <= 400:
                raise ValueError('Enter a pricing approval reason (4–400 characters).')
            if values == old:
                raise ValueError('No prices have changed.')
            db.session.add(ProTradePricingRevision(user_id=current_user.id, values=values, reason=reason))
            db.session.flush()  # Guarded tariff changes require this transaction's audit snapshot.
            for tariff in tariffs:
                tariff.base_token_cost = values['tokens'][tariff.action_name]
            db.session.commit()
            flash('Approved prices apply to new generations only. Existing receipts are unchanged.', 'success')
            return redirect(url_for('mechanic_bp.protrade_pricing'))
        except (ValueError, InvalidOperation) as exc:
            db.session.rollback()
            error, status = str(exc), 400
    history = ProTradePricingRevision.query.order_by(ProTradePricingRevision.id.desc()).limit(100).all()
    return render_template('program_mechanic/pricing.html', prices=current_prices(), labels=LABELS,
        suggestions=SUGGESTIONS, history=history, revision=history[0].id if history else 0, error=error), status


def register_routes(bp):
    bp.add_url_rule('/mechanic/admin/pricing', 'protrade_pricing', login_required(pricing_page), methods=['GET','POST'])
    bp.context_processor(lambda: dict(protrade_prices=current_prices()))
