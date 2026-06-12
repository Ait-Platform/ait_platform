import os
import sys
import logging
from flask import current_app, request
from flask import render_template as flask_render_template
# app/utils/audit_parser.py
import re
from collections import defaultdict

# --- Safe symbol handling ---
def safe_arrow():
    enc = sys.stdout.encoding or "utf-8"
    if enc.lower().startswith("utf-8"):
        return "←"   # Unicode arrow
    return "<-"     # ASCII fallback

# --- Decorator for helpers ---
def audit_helper(func):
    def wrapper(*args, **kwargs):
        current_app.logger.info(f"[AUDIT] Helper used: {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

# --- Route auditing ---
def init_route_audit(app):
    @app.before_request
    def audit_route_entry():
        current_app.logger.info(f"[AUDIT] Route entered: {request.endpoint}")

    @app.after_request
    def audit_route_exit(resp):
        arrow = safe_arrow()
        rid = hex(id(resp))[-8:]  # simple request id
        app.logger.info(f"[AUDIT] [{rid}] {arrow} {resp.status}")
        return resp

# --- Template auditing ---
def audit_render_template(template_name, *args, **kwargs):
    current_app.logger.info(f"[AUDIT] Template rendered: {template_name}")
    return flask_render_template(template_name, *args, **kwargs)

# --- File logging setup ---
def init_audit_logging(app):
    audit_dir = os.path.join(app.instance_path, "audit")
    os.makedirs(audit_dir, exist_ok=True)

    log_path = os.path.join(audit_dir, "slug_trace.log")

    # Only add if not already present
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == log_path
               for h in app.logger.handlers):
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(logging.INFO)
        app.logger.addHandler(handler)
        app.logger.info(f"[AUDIT] Logging initialized at {log_path}") 


def parse_audit_log(log_path="instance/audit/slug_trace.log",
                    out_path="instance/audit/active_doc.md"):
    # Group routes/templates/helpers by slug
    slugs = defaultdict(lambda: {"routes": set(), "helpers": set(), "templates": set()})

    with open(log_path, encoding="utf-8") as f:
        for line in f:
            if "Helper used:" in line:
                helper = line.split("Helper used:")[1].strip()
                slugs["global"]["helpers"].add(helper)

            elif "Route entered:" in line:
                route = line.split("Route entered:")[1].strip()
                # crude slug extraction: prefix before "_bp"
                slug = route.split("_bp")[0]
                slugs[slug]["routes"].add(route)

            elif "Template rendered:" in line:
                tmpl = line.split("Template rendered:")[1].strip()
                slug = tmpl.split("/")[0] if "/" in tmpl else "global"
                slugs[slug]["templates"].add(tmpl)

    with open(out_path, "w", encoding="utf-8") as out:
        out.write("# Active Audit Reference\n\n")
        for slug, data in slugs.items():
            out.write(f"## Slug: {slug}\n")
            if data["routes"]:
                out.write("### Routes\n")
                for r in sorted(data["routes"]):
                    out.write(f"- {r}\n")
            if data["helpers"]:
                out.write("\n### Helpers\n")
                for h in sorted(data["helpers"]):
                    out.write(f"- {h}\n")
            if data["templates"]:
                out.write("\n### Templates\n")
                for t in sorted(data["templates"]):
                    out.write(f"- {t}\n")
            out.write("\n")

    return out_path

