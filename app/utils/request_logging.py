"""Redact credential values before generic request fields reach a logger."""
import re


_REDACTED = "[REDACTED]"
_SENSITIVE_NAMES = {
    "code", "accesscode", "joincode", "invitationcode", "voucher", "vouchercode",
    "journey", "handover", "pin", "otp", "pwd", "passphrase", "authorization",
    "cookie", "session", "sessionid", "requestkey", "recoverycode", "backupcode",
}
_SENSITIVE_PARTS = (
    "password", "passwd", "csrf", "token", "secret", "credential",
    "apikey", "accesskey", "privatekey", "signingkey",
)


def redacted_fields(fields):
    """Return a logging-only copy; never modify request data or retain secrets."""
    result = {}
    for key, value in fields.items():
        normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
        sensitive = normalized in _SENSITIVE_NAMES or any(
            part in normalized for part in _SENSITIVE_PARTS)
        result[key] = _REDACTED if sensitive else value
    return result
