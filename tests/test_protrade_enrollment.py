"""Focused tests for ProTrade enrollment resolution and Bridge access."""
import ast
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch
from flask import Blueprint, Flask, current_app, flash, redirect, render_template, request, session, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user
from flask_wtf import CSRFProtect

ROOT = Path(__file__).resolve().parents[1]


class TestUser(UserMixin):
    def __init__(self, user_id=645, email="testuser645@example.invalid", role="user", active=True):
        self.id = user_id
        self.email = email
        self.name = "ProTrade Test User"
        self.role = role
        self.active = active

    @property
    def is_active(self):
        return self.active


class ProTradeEnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.user = TestUser(user_id=645, role="user", active=True)
        self.app = Flask("test-protrade-enrollment")
        self.app.config.update(TESTING=True, SECRET_KEY="synthetic-test-key", WTF_CSRF_ENABLED=False)
        CSRFProtect(self.app)

        manager = LoginManager(self.app)
        manager.user_loader(lambda uid: self.user if str(uid) == str(self.user.id) else None)

        self.subjects = []
        self.enrollments = []
        self.ensure_free_mock = Mock()
        self.rendered_context = None

        def mock_render(template_name, **context):
            self.rendered_context = context
            return f"rendered:{template_name}"

        # Mock modules for bridge route
        crm = Mock()
        crm.filter_by.return_value.first.return_value = None
        modules = {
            name: types.ModuleType(name)
            for name in (
                "app", "app.auth", "app.auth.routes", "app.bridge", "app.bridge.helpers",
                "app.sace_activity", "app.models", "app.models.auth", "app.models.practice_crm",
                "app.extensions", "app.utils", "app.utils.enrollment", "app.quote", "app.quote.routes"
            )
        }
        modules["app.sace_activity"].resolve_response = Mock(return_value=None)
        modules["app.models.practice_crm"].CrmPracticeUser = types.SimpleNamespace(query=crm)
        modules["app.utils.enrollment"].ensure_free_enrollment = self.ensure_free_mock
        modules["app.quote.routes"].get_baton_context = Mock(return_value={})

        module_patch = patch.dict(sys.modules, modules)
        module_patch.start()
        self.addCleanup(module_patch.stop)

        # Setup namespace for bridge_dashboard execution
        bridge_bp = Blueprint("bridge_bp", __name__)
        modes_bp = Blueprint("modes_bp", __name__)
        ns = dict(
            bridge_bp=bridge_bp,
            modes_bp=modes_bp,
            current_app=current_app,
            current_user=current_user,
            request=request,
            session=session,
            flash=flash,
            redirect=redirect,
            render_template=mock_render,
            url_for=url_for,
            login_required=login_required,
            check_admin=lambda email: False,
            get_all_subjects=lambda: self.subjects,
            get_user_enrollment=lambda uid: self.enrollments,
            ensure_free_enrollment=self.ensure_free_mock,
        )

        bridge_tree = ast.parse((ROOT / "app/bridge/routes.py").read_text(encoding="utf-8-sig"))
        bridge_fn = next(n for n in bridge_tree.body if isinstance(n, ast.FunctionDef) and n.name == "bridge_dashboard")
        exec(compile(ast.Module(body=[bridge_fn], type_ignores=[]), "actual-bridge-fn", "exec"), ns)

        # Setup namespace for modes_checkpoint_route execution
        modes_tree = ast.parse((ROOT / "app/modes/routes.py").read_text(encoding="utf-8-sig"))
        modes_fn = next(n for n in modes_tree.body if isinstance(n, ast.FunctionDef) and n.name == "modes_checkpoint_route")
        
        self.mock_subj_query = Mock()
        self.mock_enr_query = Mock()
        self.mock_user_query = Mock()
        modes_ns = dict(
            modes_bp=modes_bp,
            bridge_bp=bridge_bp,
            current_app=current_app,
            current_user=current_user,
            request=request,
            session=session,
            flash=flash,
            redirect=redirect,
            render_template=mock_render,
            url_for=url_for,
            login_required=login_required,
            check_admin=lambda email: False,
            get_all_subjects=lambda: self.subjects,
            get_user_enrollment=lambda uid: self.enrollments,
            ensure_free_enrollment=self.ensure_free_mock,
            AuthSubject=types.SimpleNamespace(query=self.mock_subj_query),
            UserEnrollment=types.SimpleNamespace(query=self.mock_enr_query),
            User=types.SimpleNamespace(query=self.mock_user_query),
            modes_checkpoint=Mock(return_value=[]),
            expiry_for=Mock(return_value=None),
            app_now=Mock(),
            timezone=types.SimpleNamespace(utc=None),
            db=types.SimpleNamespace(session=Mock()),
        )
        exec(compile(ast.Module(body=[modes_fn], type_ignores=[]), "actual-modes-fn", "exec"), modes_ns)

        self.app.register_blueprint(bridge_bp)
        self.app.register_blueprint(modes_bp)

        self.app.add_url_rule("/mode/<subject_slug>", endpoint="modes_bp.modes_checkpoint_route", view_func=modes_ns["modes_checkpoint_route"])
        self.app.add_url_rule("/price/<subject>", endpoint="payment_bp.pricing_base", view_func=lambda subject: f"price:{subject}")
        self.app.add_url_rule("/mechanic/dashboard", endpoint="mechanic_bp.mechanic_dashboard", view_func=lambda: "mechanic_dashboard")
        self.app.add_url_rule("/cultural-fire", endpoint="cultural_bp.cultural_fire_router", view_func=lambda: "cultural")

        self.client = self.app.test_client()

    def test_user_645_active_protrade_enrollment_resolves_cleanly(self):
        """Active user 645 with active ProTrade enrollment gets enrolled access on Bridge without errors."""
        self.subjects = [
            types.SimpleNamespace(
                id=24,
                slug="mechanic",
                name="ProTrade",
                program_type="paid",
                commercial_mode="free",
                is_hidden_on_bridge=False,
            )
        ]
        self.enrollments = [
            types.SimpleNamespace(
                id=101,
                user_id=645,
                subject_id=24,
                status="active",
            )
        ]

        with self.app.test_request_context("/bridge"):
            login_user(self.user)
            self.app.view_functions["bridge_bp.bridge"]()

            self.assertIsNotNone(self.rendered_context)
            tiles = self.rendered_context.get("subjects", [])
            self.assertEqual(len(tiles), 1)
            tile = tiles[0]
            self.assertEqual(tile["slug"], "mechanic")
            self.assertEqual(tile["name"], "ProTrade")
            self.assertEqual(tile["access_level"], "enrolled")
            self.assertEqual(tile["message"], "Your paid program is active.")
            self.assertEqual(tile["href"], "/mode/mechanic")

            # Ensure free enrollment was NOT auto-granted (user_enrollment is source of truth)
            self.ensure_free_mock.assert_not_called()

            # Ensure no error flash
            flashes = session.get("_flashes", [])
            self.assertEqual(len(flashes), 0)

    def test_user_645_pending_protrade_enrollment_remains_locked(self):
        """Pending ProTrade enrollment remains locked and does not grant access automatically."""
        self.subjects = [
            types.SimpleNamespace(
                id=24,
                slug="mechanic",
                name="ProTrade",
                program_type="paid",
                commercial_mode="free",
                is_hidden_on_bridge=False,
            )
        ]
        self.enrollments = [
            types.SimpleNamespace(
                id=102,
                user_id=645,
                subject_id=24,
                status="pending",
            )
        ]

        with self.app.test_request_context("/bridge"):
            login_user(self.user)
            self.app.view_functions["bridge_bp.bridge"]()

            self.assertIsNotNone(self.rendered_context)
            tiles = self.rendered_context.get("subjects", [])
            self.assertEqual(len(tiles), 1)
            tile = tiles[0]
            self.assertEqual(tile["slug"], "mechanic")
            self.assertEqual(tile["access_level"], "locked")
            self.assertEqual(tile["href"], "/price/mechanic")

            # Do not bypass enrollment checks or grant access automatically
            self.ensure_free_mock.assert_not_called()

            # Ensure no "Enrollment state could not be resolved" error flash
            flashes = session.get("_flashes", [])
            for category, message in flashes:
                self.assertNotIn("could not be resolved", message)

    def test_modes_checkpoint_route_protrade_active_vs_unenrolled(self):
        """modes_checkpoint_route redirects active users to program, and blocks unenrolled users."""
        subj = types.SimpleNamespace(
            id=24,
            slug="mechanic",
            name="ProTrade",
            program_type="paid",
            commercial_mode="free",
            start_endpoint="mechanic_bp.mechanic_dashboard",
        )
        active_enrollment = types.SimpleNamespace(
            id=101,
            user_id=645,
            subject_id=24,
            status="active",
        )

        # 1. With active enrollment -> redirects to mechanic_bp.mechanic_dashboard
        self.mock_subj_query.filter_by.return_value.first_or_404.return_value = subj
        self.mock_enr_query.filter_by.return_value.first.return_value = active_enrollment
        with self.app.test_request_context("/mode/mechanic?subject_slug=mechanic"):
            login_user(self.user)
            resp = self.app.view_functions["modes_bp.modes_checkpoint_route"]()
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.location, "/mechanic/dashboard")

        # 2. With pending enrollment -> does NOT grant access, redirects to bridge
        pending_enrollment = types.SimpleNamespace(
            id=102,
            user_id=645,
            subject_id=24,
            status="pending",
        )
        self.mock_enr_query.filter_by.return_value.first.return_value = pending_enrollment
        with self.app.test_request_context("/mode/mechanic?subject_slug=mechanic"):
            login_user(self.user)
            resp = self.app.view_functions["modes_bp.modes_checkpoint_route"]()
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.location, "/bridge")

        # 3. Unenrolled (None) -> does NOT grant access, redirects to bridge
        self.mock_enr_query.filter_by.return_value.first.return_value = None
        with self.app.test_request_context("/mode/mechanic?subject_slug=mechanic"):
            login_user(self.user)
            resp = self.app.view_functions["modes_bp.modes_checkpoint_route"]()
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.location, "/bridge")


if __name__ == "__main__":
    unittest.main(verbosity=2)
