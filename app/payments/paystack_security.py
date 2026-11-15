"""Verify gateway responses and bind new checkouts to server-approved terms."""
import json
import os
from urllib.parse import quote
import requests
from flask import current_app
from itsdangerous import URLSafeSerializer, BadSignature


def metadata_for(transaction):
    metadata = transaction.get('metadata') or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            raise ValueError('Invalid payment metadata') from None
    if not isinstance(metadata, dict):
        raise ValueError('Invalid payment metadata')
    return metadata


def order_signer():
    return URLSafeSerializer(current_app.secret_key, salt='paystack-order-v1')


def signed_order(reference, email, subject, amount, currency, tokens):
    return order_signer().dumps(dict(reference=reference, email=email, subject=subject,
        amount=amount, currency=currency, tokens=tokens))


def verified_transaction(reference, secret):
    if not isinstance(reference, str) or not reference or len(reference) > 200 or not secret:
        raise ValueError('Invalid payment reference')
    response = requests.get('https://api.paystack.co/transaction/verify/' + quote(reference, safe=''),
        headers={'Authorization': 'Bearer ' + secret}, timeout=15)
    response.raise_for_status()
    payload = response.json()
    transaction = payload.get('data') or {}
    if payload.get('status') is not True or transaction.get('status') != 'success':
        raise ValueError('Payment is not successful')
    if transaction.get('reference') != reference:
        raise ValueError('Payment reference does not match')
    amount = transaction.get('amount')
    if type(amount) is not int or amount <= 0:
        raise ValueError('Invalid verified payment amount')
    currency = transaction.get('currency')
    if not isinstance(currency, str) or len(currency) != 3:
        raise ValueError('Invalid verified payment currency')
    if os.environ.get('RENDER') == 'true' and transaction.get('domain') != 'live':
        raise ValueError('Live payments are required for production fulfillment')
    metadata = metadata_for(transaction)
    email = str(metadata.get('email') or (transaction.get('customer') or {}).get('email') or '').strip().lower()
    subject = str(metadata.get('subject') or '').strip().lower()
    if not email or not subject:
        raise ValueError('Missing verified order identity')
    tokens = metadata.get('tokens_purchased')
    if subject.endswith('_topup'):
        if tokens is None:
            tokens = amount // 100
        if isinstance(tokens, str) and tokens.isdecimal():
            tokens = int(tokens)
        if type(tokens) is not int or tokens <= 0:
            raise ValueError('Invalid token quantity')
    order_token = metadata.get('order_token')
    if order_token:
        try:
            expected = order_signer().loads(order_token)
        except BadSignature:
            raise ValueError('Invalid payment order signature') from None
        actual = dict(reference=reference, email=email, subject=subject, amount=amount,
            currency=currency, tokens=tokens)
        if expected != actual:
            raise ValueError('Verified payment does not match the checkout order')
    metadata['tokens_purchased'] = tokens
    transaction['metadata'] = metadata
    return email, subject, transaction
