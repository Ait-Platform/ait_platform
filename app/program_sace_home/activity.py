"""HOME adapter; no generic roles, enrollments or session authority."""
from flask import request, url_for
from . import continuation, lifecycle, service


class HomeActivity:
    identifier = 'home'
    display_name = 'Hands-On Math Education'

    def recognizes(self, path, subject):
        return subject == service.SUBJECT or bool(path and path.startswith('/sace/home/'))

    def has_authority(self):
        from .auth import lifecycle_available
        return lifecycle_available() and bool(lifecycle.appointment() or lifecycle.assignments())

    def entry(self, target=None, *, selected=False):
        from app.sace_activity import local_path
        if selected and request.endpoint == 'auth_bp.login':
            from .auth import record_signin
            record_signin()
        if target:
            continuation.clear()
            if local_path(target) != '/sace/home/':
                return target
        return url_for('home_sace_bp.control' if service.controller() else 'home_sace_bp.entry')

    def continuation(self, target=None, *, selected=False):
        from app.sace_activity import local_path
        if target and local_path(target) not in ('/sace/home/claim', '/sace/home/provisioning'):
            return None
        # Validation uses HOME's invitation/provisioning ownership and consent.
        destination = continuation.consume() if selected else continuation.resolve()
        if destination and selected and request.endpoint == 'auth_bp.login':
            from .auth import record_signin
            record_signin()
        return url_for(destination) if destination else None
