# app/diagnostics.py
import logging, traceback, functools
from flask import request
from flask.signals import template_rendered, got_request_exception

def _stack(label: str) -> str:
    return (
        f"\n--- STACK {label} ---\n"
        + "".join(traceback.format_stack(limit=80))
        + "\n--- END STACK ---"
    )

def _short(x, n=60):
    try:
        s = str(x)
        return (s[:n] + "…") if len(s) > n else s
    except Exception:
        return repr(x)

def install_request_trace(app):
    log = app.logger or logging.getLogger("app")

    @app.before_request
    def _trace_request():
        rule = request.url_rule.rule if request.url_rule else None
        ep   = request.url_rule.endpoint if request.url_rule else None
        bp   = request.blueprint
        log.info(
            "[TRACE] %s %s rule=%r endpoint=%r bp=%r view_args=%r args=%r",
            request.method, request.path, rule, ep, bp,
            dict(request.view_args or {}), dict(request.args or {}),
        )

    def _on_template(sender, template, context, **extra):
        ep = request.url_rule.endpoint if request.url_rule else None
        item = context.get("item")
        buttons = context.get("buttons")
        # Try to peek into item fields without assuming it exists
        try:
            title   = getattr(item, "title", None) or (item.get("title") if isinstance(item, dict) else None)
            caption = getattr(item, "caption", None) or (item.get("caption") if isinstance(item, dict) else None)
            content = getattr(item, "content", None) or (item.get("content") if isinstance(item, dict) else None)
        except Exception:
            title = caption = content = None

        log.info(
            "[TRACE] template=%s endpoint=%r ctx_keys=%s item?=%s buttons?=%s title=%r caption=%r content=%r",
            getattr(template, "name", repr(template)), ep,
            ", ".join(list(context.keys())[:20]),
            bool(item), bool(buttons),
            _short(title), _short(caption), _short(content),
        )

    template_rendered.connect(_on_template, app)

    def _on_exception(sender, exception, **extra):
        log.exception("[TRACE] exception raised: %s", exception)

    got_request_exception.connect(_on_exception, app)

def trace_route(label: str = None):
    """Decorator that logs entry + call stack. No assumptions about local vars."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            from flask import current_app
            log = current_app.logger
            ep  = f"{current_app.blueprints.get(request.blueprint).name}.{fn.__name__}" if request.blueprint else fn.__name__
            tag = label or ep
            log.info("[CALL] %s path=%s view_args=%r args=%r kwargs=%r",
                     tag, request.path, dict(request.view_args or {}), args, kwargs)
            log.warning(_stack(tag))
            return fn(*args, **kwargs)
        return wrapper
    return deco
