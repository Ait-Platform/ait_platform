"""Reading adapter; all authority remains in Reading's lifecycle predicates."""
import hashlib
import time
from flask import current_app, request, session, url_for
from flask_login import current_user
from . import access, endorsement as flow


class ReadingActivity:
    identifier = 'reading'
    display_name = 'Reading'

    def recognizes(self, path, subject):
        if subject == access.SUBJECT:
            return True
        if not path:
            return False
        from werkzeug.exceptions import HTTPException
        try:
            rule, _ = current_app.url_map.bind_to_environ(request.environ).match(
                path, method='GET', return_rule=True)
        except HTTPException:
            return False
        return rule.endpoint.startswith('sace_bp.')

    def has_authority(self):
        return bool(access.is_controller() or flow.assignments(current_user.id, active_only=True))

    def entry(self, target=None, *, selected=False):
        from app.sace_activity import local_path
        if not current_user.is_authenticated:
            return url_for('sace_bp.dashboard')
        path = local_path(target)
        if target and path not in ('/sace/dashboard', '/sace/reading', '/sace/claim_code', '/sace/provisioning'):
            return target
        if access.is_controller():
            return url_for('sace_bp.provisioning_map')
        if path == '/sace/provisioning':
            return None
        if flow.assignments(current_user.id, active_only=True):
            return url_for('sace_bp.reading_hub')
        return target or url_for('sace_bp.dashboard')

    def continuation(self, target=None, *, selected=False):
        from app.sace_activity import local_path
        # An existing auditor assignment wins over residual onboarding state.
        # Session continuations must never turn an auditor into a controller.
        if not access.is_controller() and flow.assignments(current_user.id, active_only=True):
            access.clear_provisioning()
            return None
        if target and local_path(target) == '/sace/provisioning':
            if access.provisioning_auth_context(target):
                if not selected or access.authenticate_provisioning(target):
                    return target
            return None
        if target and local_path(target) != '/sace/claim_code':
            return None
        context = access.provisioning_context()
        if context and context.get('accepted_at'):
            from app.models.sace import SaceWorkshopInteraction
            used = SaceWorkshopInteraction.query.filter(
                SaceWorkshopInteraction.activity_slug == 'controller_provisioned',
                SaceWorkshopInteraction.response_data.contains(context['nonce'], autoescape=True)).first()
            if used:
                access.clear_provisioning()
            else:
                destination = url_for('sace_bp.provisioning_map', journey=context['nonce'])
                if not selected or access.authenticate_provisioning(destination):
                    return destination
        context = session.get('sace_reading_auditor_journey')
        code = session.get('pending_sace_code', '')
        if (isinstance(code, str) and isinstance(context, dict)
                and isinstance(context.get('expires_at'), (int, float))
                and context['expires_at'] > time.time()
                and context.get('user_id') in (None, current_user.id)
                and context.get('code_hash') == hashlib.sha256(code.encode()).hexdigest()
                and session.get('sace_evaluator_pledged')):
            from app.models.sace import SaceWorkshopInteraction
            rows = SaceWorkshopInteraction.query.filter_by(activity_slug='auditor_provisioned').all()
            invitation = next((row for row in rows if flow.payload(row).get('code') == code), None)
            if flow.invitation_error(invitation) is None:
                if selected:
                    session.pop('sace_reading_auditor_journey', None)
                return url_for('sace_bp.claim_code')
        if target:
            # The guarded claim must report genuine invitation/lifecycle 409s.
            return None
        session.pop('sace_reading_auditor_journey', None)
        session.pop('pending_sace_code', None)
        session.pop('sace_evaluator_pledged', None)
        return None
