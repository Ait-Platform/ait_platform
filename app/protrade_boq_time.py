"""Server-clock BOQ preparation leases. No wallet, ledger or work-register writes."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from flask import abort, jsonify, render_template, request
from flask_login import current_user, login_required
from flask_wtf.csrf import generate_csrf
from app.extensions import db
from app.models.mechanic import MechShop, ProTradeBoqQuote, ProTradeBoqPreparation
from app.models.debtors import Debtor
from app.models.auth import User
from app.utils.roles import is_admin

LEASE_SECONDS = 20


def settle(row, now):
    if row.last_seen_at is not None:
        gap = (now - row.last_seen_at).total_seconds()
        elapsed = gap if 0 <= gap <= LEASE_SECONDS else 0
        row.total_seconds = Decimal(row.total_seconds or 0) + Decimal(str(elapsed)).quantize(Decimal('.001'))


def attach_quote(quote):
    if quote.trade != 'boq':
        return
    row = ProTradeBoqPreparation.query.filter_by(user_id=quote.user_id,
        shop_id=quote.shop_id, debtor_id=quote.debtor_id,
        quote_key=quote.quote_number.removeprefix('CAR-Q-').lower()).with_for_update().first()
    if row:
        row.quote_id = quote.id


def complete_quote(quote):
    if quote.trade != 'boq':
        return
    row = ProTradeBoqPreparation.query.filter_by(user_id=quote.user_id, quote_id=quote.id).with_for_update().first()
    if row and row.completed_at is None:
        now = datetime.utcnow()
        settle(row, now)
        row.completed_at = now
        row.last_seen_at = None


def heartbeat(shop_id, debtor_id):
    shop = MechShop.query.filter_by(id=shop_id, user_id=current_user.id, trade='boq').first()
    # Same lock order as quote saves; serializes first insert and attachment.
    account = Debtor.query.filter_by(id=debtor_id, shop_id=shop_id, user_id=current_user.id)
    debtor = (account.with_for_update() if request.method == 'POST' else account).first()
    if not shop or not debtor:
        abort(404)
    if request.method == 'GET':
        response = jsonify(csrf=generate_csrf())
        response.headers['Cache-Control'] = 'no-store'
        return response
    try:
        key = UUID(request.form['quote_key']).hex
        tab = UUID(request.form['tab_key']).hex
        tick = int(request.form['tick'])
        if not 0 < tick < 2147483647:
            raise ValueError()
    except (KeyError, ValueError, AttributeError):
        abort(400)
    action = request.form.get('action')
    if action not in ('active', 'pause'):
        abort(400)
    quote = ProTradeBoqQuote.query.filter_by(user_id=current_user.id, shop_id=shop_id,
        debtor_id=debtor_id, trade='boq', quote_number='CAR-Q-' + key.upper()).first()
    # Keys already belonging to another account must never become parallel timers.
    conflicting = ProTradeBoqPreparation.query.filter_by(user_id=current_user.id, quote_key=key).first()
    if conflicting and (conflicting.shop_id != shop_id or conflicting.debtor_id != debtor_id):
        abort(404)
    if not quote and ProTradeBoqQuote.query.filter_by(quote_number='CAR-Q-' + key.upper()).first():
        abort(404)
    row = ProTradeBoqPreparation.query.filter_by(user_id=current_user.id, quote_key=key).with_for_update().first()
    now = datetime.utcnow()
    if row is None:
        if action == 'pause' or (quote and not quote.is_draft):
            return ('', 204)
        row = ProTradeBoqPreparation(user_id=current_user.id, shop_id=shop_id, debtor_id=debtor_id,
            quote_key=key, quote_id=quote.id if quote else None, started_at=now, total_seconds=0, tick=0)
        db.session.add(row)
    if row.completed_at is not None or (quote and not quote.is_draft):
        return ('', 204)
    if quote:
        row.quote_id = quote.id
    if row.tab_key == tab:
        if tick <= row.tick:
            return ('', 204)
        settle(row, now)
    elif row.last_seen_at and (now - row.last_seen_at).total_seconds() <= LEASE_SECONDS:
        return ('', 204)  # Another tab owns this short lease; never double-count.
    elif action == 'pause':
        return ('', 204)
    row.tab_key, row.tick = tab, tick
    row.last_seen_at = now if action == 'active' else None
    db.session.commit()
    return ('', 204)


def report():
    if not is_admin(current_user):
        abort(403)
    page = request.args.get('page', 1, type=int)
    if page is None or page < 1:
        abort(400)
    rows = db.session.query(ProTradeBoqPreparation, MechShop, User, ProTradeBoqQuote, Debtor).join(
        MechShop, MechShop.id == ProTradeBoqPreparation.shop_id).join(
        User, User.id == ProTradeBoqPreparation.user_id).join(
        Debtor, Debtor.id == ProTradeBoqPreparation.debtor_id).outerjoin(
        ProTradeBoqQuote, ProTradeBoqQuote.id == ProTradeBoqPreparation.quote_id
    ).order_by(ProTradeBoqPreparation.started_at.desc(), ProTradeBoqPreparation.id.desc()).offset((page-1)*100).limit(101).all()
    return render_template('program_mechanic/boq_time_report.html', rows=rows[:100], page=page, has_next=len(rows)>100)


def register_routes(bp):
    bp.add_url_rule('/mechanic/shop/<int:shop_id>/debtor/<int:debtor_id>/boq-preparation',
        'boq_preparation', login_required(heartbeat), methods=['GET', 'POST'])
    bp.add_url_rule('/mechanic/admin/boq-preparation', 'boq_preparation_report', login_required(report))
