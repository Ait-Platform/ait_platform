"""Client work entries with immutable original records and appended corrections."""
from datetime import date, datetime
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
from flask import abort, render_template, request, redirect, url_for
from flask_login import current_user
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.debtors import Debtor
from app.models.mechanic import MechShop, ProTradeWorkActivity


def activity_page(shop_id, debtor_id):
    shop = MechShop.query.filter_by(id=shop_id, user_id=current_user.id).first_or_404()
    debtor = Debtor.query.filter_by(id=debtor_id, shop_id=shop.id, user_id=current_user.id).first_or_404()
    query = ProTradeWorkActivity.query.filter_by(user_id=current_user.id, shop_id=shop.id, debtor_id=debtor.id)
    if request.method == 'POST':
        try:
            work_date = date.fromisoformat(request.form.get('work_date', ''))
            description = request.form.get('description', '').strip()
            if not description or len(description) > 4000:
                raise ValueError()
        except ValueError:
            abort(400, description='Enter a valid work date and a description of up to 4000 characters.')
        previous_id = request.form.get('previous_id', type=int)
        if request.form.get('previous_id') and previous_id is None:
            abort(400)
        previous = query.filter_by(id=previous_id).populate_existing().with_for_update().first_or_404() if previous_id else None
        if previous:
            entry_key = previous.entry_key
            revision = previous.revision + 1
        else:
            try:
                entry_key = UUID(request.form.get('entry_key', '')).hex
            except (ValueError, AttributeError):
                abort(400)
            revision = 1
        existing = query.filter_by(entry_key=entry_key, revision=revision).first()
        if existing:
            if (existing.work_date, existing.description) != (work_date, description):
                abort(409, description='This work entry has already been saved. Open its latest revision to correct it.')
        else:
            record = ProTradeWorkActivity(user_id=current_user.id, shop_id=shop.id, debtor_id=debtor.id,
                entry_key=entry_key, revision=revision, previous_id=previous.id if previous else None,
                work_date=work_date, description=description)
            try:
                db.session.add(record)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                abort(409, description='The work entry changed while saving. Reload its current revision.')
        return redirect(url_for('mechanic_bp.work_activity', shop_id=shop.id, debtor_id=debtor.id))
    records = query.order_by(ProTradeWorkActivity.recorded_at, ProTradeWorkActivity.id).all()
    history = {}
    for record in records:
        history.setdefault(record.entry_key, []).append(record)
    edit_id = request.args.get('edit_id', type=int)
    if request.args.get('edit_id') and edit_id is None:
        abort(404)
    editing = query.filter_by(id=edit_id).first_or_404() if edit_id else None
    if editing and history[editing.entry_key][-1].id != editing.id:
        abort(409, description='Open the latest revision to correct this work entry.')
    return render_template('program_mechanic/work_activity.html', shop=shop, debtor=debtor,
        history=history, editing=editing, entry_key=uuid4().hex,
        today=datetime.now(ZoneInfo('Africa/Johannesburg')).date())
