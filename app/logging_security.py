"""Redact credentials at the application logger boundary, without changing requests."""
import logging
import re
import traceback
from urllib.parse import parse_qs, quote, quote_plus, urlsplit

from flask import has_request_context, request, session
from werkzeug.exceptions import HTTPException


SECRET_KEYS = frozenset({
    "journey", "nonce", "token", "code", "password", "confirm_password",
    "password_confirm", "csrf_token", "signature", "password_hash",
})
_QUERY_SECRET = re.compile(
    r"((?:[?&]|%(?:25)*3f|%(?:25)*26)(?:" + "|".join(SECRET_KEYS) + r")(?:=|%(?:25)*3d))"
    r"((?:(?!%(?:25)*26)[^&\s\"'<>])*)", re.IGNORECASE,
)


def redact_url_values(message):
    # Also handles query parameters inside percent-encoded authentication next URLs.
    return _QUERY_SECRET.sub(lambda match: match[1] + "[REDACTED]", message)


class RequestSecretFilter(logging.Filter):
    def filter(self, record):
        values = set()

        def collect(mapping, depth=0):
            for key, value in mapping.items():
                if isinstance(value, dict):
                    collect(value, depth)
                elif key.lower() == "next" and depth < 5:
                    for target in value if isinstance(value, (list, tuple)) else (value,):
                        try:
                            collect(parse_qs(urlsplit(str(target)).query), depth + 1)
                        except ValueError:
                            pass
                elif key.lower() in SECRET_KEYS or key in {
                    "sace_home_provisioning_token", "sace_home_pending_code",
                }:
                    for item in value if isinstance(value, (list, tuple)) else (value,):
                        if item:
                            values.add(str(item))

        if has_request_context():
            collect(request.args.to_dict(flat=False))
            collect(request.view_args or {})
            try:
                collect(request.form.to_dict(flat=False))
            except HTTPException:
                # Malformed bodies must not prevent the original error being logged.
                pass
            # Includes current nonces when errors occur after reading session context.
            collect({key: value for key, value in session.items()
                     if key.startswith(("sace_home_", "sace_r_"))})

        def clean(message):
            for value in sorted(values, key=len, reverse=True):
                for variant in {value, repr(value)[1:-1], quote(value, safe=""),
                                quote_plus(value, safe="")}:
                    message = message.replace(variant, "[REDACTED]")
            return redact_url_values(message)

        record.msg = clean(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = clean("".join(traceback.format_exception(*record.exc_info)))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = clean(record.exc_text)
        if record.stack_info:
            record.stack_info = clean(record.stack_info)
        return True
