"""Shared personal details, with existing business account identities preserved."""
from flask import abort
from flask_login import current_user
from sqlalchemy import func, or_
from app.models.debtors import Debtor
from app.models.mechanic import MechShop, MechClient


def update_directory_details(identity, values):
    old_key = (identity.name.lower(), identity.phone or '', identity.email or '')
    new_key = (values['name'].lower(), values['phone'], values['email'])
    if new_key != old_key:
        duplicate_identity = Debtor.query.filter(Debtor.user_id == current_user.id,
            Debtor.identity_debtor_id.is_(None), Debtor.id != identity.id,
            func.lower(Debtor.name) == new_key[0], func.coalesce(Debtor.phone, '') == new_key[1],
            func.coalesce(Debtor.email, '') == new_key[2]).first()
        if duplicate_identity:
            return False
    accounts = Debtor.query.filter(Debtor.user_id == current_user.id,
        or_(Debtor.id == identity.id, Debtor.identity_debtor_id == identity.id)).populate_existing().all()
    shop_ids = {account.shop_id for account in accounts if account.shop_id is not None}
    if shop_ids:
        owned = MechShop.query.filter(MechShop.user_id == current_user.id, MechShop.id.in_(shop_ids)).count()
        if owned != len(shop_ids):
            abort(409, description='A linked account has an invalid business association.')
        conflict = Debtor.query.filter(Debtor.user_id == current_user.id,
            Debtor.shop_id.in_(shop_ids), Debtor.id.notin_([account.id for account in accounts]),
            func.lower(Debtor.name) == values['name'].lower(),
            func.coalesce(Debtor.phone, '') == values['phone'],
            func.coalesce(Debtor.email, '') == values['email']).first()
        if conflict:
            return False
    clients = []
    for account in accounts:
        if account.shop_id is None:
            continue
        client = MechClient.query.filter_by(id=account.reference_id, shop_id=account.shop_id,
            user_id=current_user.id).first()
        if client is None:
            abort(409, description='A linked business account has an invalid client association.')
        clients.append(client)
    # Validate the complete group before changing contact details. Financial rows,
    # account IDs, ownership and historical document snapshots keep their identity.
    for record in accounts + clients:
        for field, value in values.items():
            setattr(record, field, value)
    return True
