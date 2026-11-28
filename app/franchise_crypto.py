"""Versioned profile protection: shared AIT writes and legacy ciphertext reads."""
import base64
import hashlib
import hmac
from cryptography.fernet import Fernet, InvalidToken
from flask import current_app

PREFIX = 'ait-shared-v1:'

class ProfileProtectionError(RuntimeError):
    pass


def shared_key():
    secret = current_app.config.get('SECRET_KEY')
    if not secret or secret == 'dev-secret':
        raise ProfileProtectionError('AIT secure configuration is unavailable. No profile was saved. Contact AIT Admin.')
    secret = secret.encode() if isinstance(secret,str) else secret
    return base64.urlsafe_b64encode(hmac.new(secret,b'ait-global-franchise-profile-v1',hashlib.sha256).digest())


def encrypt_private(value):
    return PREFIX + Fernet(shared_key()).encrypt(value.encode()).decode()


def legacy_keys():
    value = current_app.config.get('FRANCHISEE_PII_KEY')
    if value:
        try:
            key = value.encode() if isinstance(value,str) else value
            Fernet(key)
            return [key]
        except (ValueError,TypeError):
            pass
    return []


def decrypt_private(value):
    if value.startswith(PREFIX):
        return Fernet(shared_key()).decrypt(value[len(PREFIX):].encode()).decode()
    # Untagged records may use either the original dedicated key or the earlier
    # shared-secret fallback. Never rewrite old ciphertext merely by reading it.
    keys = legacy_keys()
    try:
        keys.append(shared_key())
    except ProfileProtectionError:
        if not keys:
            raise
    for key in keys:
        try:
            return Fernet(key).decrypt(value.encode()).decode()
        except InvalidToken:
            continue
    raise InvalidToken()
