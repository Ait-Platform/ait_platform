"""Check existing franchisee settings without loading AIT or printing their values."""
import json
import os
import re
from urllib.parse import urlsplit
from cryptography.fernet import Fernet


def verify(settings):
    key_ok=False
    try:
        value=settings.get('FRANCHISEE_PII_KEY','')
        cipher=Fernet(value.encode() if isinstance(value,str) else value)
        sample=b'ait-franchisee-preflight'
        key_ok=cipher.decrypt(cipher.encrypt(sample))==sample
    except Exception:
        pass
    origin_ok=False
    try:
        value=settings.get('FRANCHISEE_PUBLIC_ORIGIN','')
        parts=urlsplit(value)
        origin_ok=bool(re.fullmatch(r'https://[A-Za-z0-9.-]+(?::[0-9]+)?/?',value)
            and parts.hostname and parts.username is None and parts.password is None
            and parts.path in ('','/') and not parts.query and not parts.fragment
            and (parts.port is None or 1<=parts.port<=65535))
    except (ValueError,TypeError):
        pass
    return {'FRANCHISEE_PII_KEY_valid':key_ok,'FRANCHISEE_PUBLIC_ORIGIN_valid':origin_ok}


if __name__=='__main__':
    checks=verify(os.environ)
    print(json.dumps(checks,sort_keys=True))
    raise SystemExit(0 if all(checks.values()) else 1)
