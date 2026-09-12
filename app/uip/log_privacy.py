"""Allowlisted UIP request-log metadata; business audit events remain untouched."""
import logging
import re
from flask import g, has_request_context, request


def _identifier(value):
    return value if type(value) is int and value > 0 else None


class UipTracePrivacy(logging.Filter):
    def filter(self, record):
        if not has_request_context():
            return True
        if request.blueprint != "uip_bp" and not (request.path == "/uip" or request.path.startswith("/uip/")):
            return True
        # Do not copy message bodies, URL values, headers or exception text into
        # request logs. This also covers JSON bodies and unexpected error logs.
        status = None
        event = "application"
        args = record.args
        if "form=%s" in str(record.msg):
            event = "request"
        elif isinstance(args, tuple) and len(args) >= 2 and (
            str(record.msg).endswith("%s redirect to %s") or str(record.msg).endswith("%s")):
            match = re.fullmatch(r"([1-5][0-9]{2})(?: [A-Za-z ]+)?", str(args[1]))
            if match:
                event, status = "response", match.group(1)
        rid = getattr(g, "reqid", "")
        if not isinstance(rid, str) or not re.fullmatch(r"[a-fA-F0-9-]{8,36}", rid):
            rid = "unavailable"
        org = getattr(g, "organization", None)
        cached_user = getattr(g, "_login_user", None)
        user_id = _identifier(getattr(request, "user_id", None)) or _identifier(getattr(cached_user, "id", None))
        route = request.url_rule.rule if request.url_rule else "(unmatched UIP route)"
        record.msg = "UIP %s request_id=%s method=%s route=%s endpoint=%s status=%s organization_id=%s user_id=%s (UIP request contents omitted)"
        org_id = getattr(g, "org_id", getattr(org, "id", None) if org and getattr(org, "_sa_instance_state", None) and not org._sa_instance_state.expired else None)
        record.args = (event, rid, request.method, route, request.endpoint, status,
                       _identifier(org_id), user_id)
        
        # Append short exception message if one exists, but strip stack traces
        if record.exc_info:
            exc_type, exc_val, _ = record.exc_info
            record.msg += f" (exception: {exc_type.__name__}: {str(exc_val)})"
            
        # Exception messages/stack values can contain source text or credentials.
        # Preserve severity; operational actions remain in UipAuditEvent.
        record.exc_info = record.exc_text = record.stack_info = None
        return True


def install(app):
    if not any(isinstance(f, UipTracePrivacy) for f in app.logger.filters):
        app.logger.addFilter(UipTracePrivacy())
