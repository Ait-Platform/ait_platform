"""Atomic document and BOQ section wallet receipts."""
from flask import abort
from werkzeug.exceptions import HTTPException
from app.extensions import db
from app.models.auth import AitTokenWallet, AitTokenTransaction
from app.models.mechanic import ProTradeDocument


DOCUMENT_TOKENS = {'quote': 10, 'invoice': 10, 'soa': 10, 'boq_section': 25}


class InsufficientTokens(HTTPException):
    code = 402
    description = 'Please top up your wallet before generating this document.'


def charge_document(user_id, shop_id, debtor_id, key, kind, snapshot=None, pdf=None):
    tokens = DOCUMENT_TOKENS.get(kind)
    if tokens is None:
        abort(400)
    # Serialize charges for this wallet across workers, including duplicate requests.
    with db.session.no_autoflush:
        wallet = AitTokenWallet.query.filter_by(user_id=user_id).populate_existing().with_for_update().first()
        existing = ProTradeDocument.query.filter_by(user_id=user_id, document_key=key).first()
        if existing:
            if (existing.shop_id, existing.debtor_id, existing.kind) != (shop_id, debtor_id, kind):
                abort(409)
            return existing
        if not wallet or wallet.balance < tokens:
            db.session.rollback()
            raise InsufficientTokens(description=f'This generation costs {tokens} tokens. Please top up your wallet.')
    wallet.balance -= tokens
    transaction = AitTokenTransaction(
        wallet_id=wallet.id, amount=-tokens,
        description=f'ProTrade {kind} {key} ({tokens} tokens)',
    )
    db.session.add(transaction)
    db.session.flush()
    document = ProTradeDocument(
        user_id=user_id, shop_id=shop_id, debtor_id=debtor_id, document_key=key,
        kind=kind, token_transaction_id=transaction.id, tokens=tokens, snapshot=snapshot, pdf=pdf,
    )
    db.session.add(document)
    db.session.flush()
    return document
