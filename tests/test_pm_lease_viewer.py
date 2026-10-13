"""Isolated PM endpoint tests: no AIT factory, database, network or conversion."""
import hashlib
import importlib.util
from html.parser import HTMLParser
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from flask import Blueprint, Flask
from flask_login import LoginManager, UserMixin, current_user
from jinja2 import DictLoader, Environment
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase

ROOT = Path(__file__).resolve().parents[1]


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    is_active = Column(Integer)


class ApprovedAdmin(Base):
    __tablename__ = "auth_approved_admin"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    active = Column(Boolean)


class Identity(UserMixin):
    def __init__(self, identifier):
        self.id = identifier


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class LeaseViewerTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="pm_viewer_", dir=ROOT / "tmp")
        self.addCleanup(self.temp.cleanup)
        self.private = Path(self.temp.name) / "instance" / "pm_lease_documents"
        (self.private / "R01").mkdir(parents=True)
        self.original = {}
        for suffix in ("docx", "pdf"):
            name = "Lease_Review_Draft_R01." + suffix
            source = ROOT / "output" / "pm_stage5" / name
            self.original[name] = source.read_bytes()
            shutil.copyfile(source, self.private / "R01" / name)
        self.user_active = {1: True, 2: True, 3: True, 4: False}
        self.grants = {1: True, 2: False, 3: False, 4: True}
        self.database = types.SimpleNamespace(session=types.SimpleNamespace(execute=Mock(side_effect=self.authorize)))
        blueprint = Blueprint("admin_bp", __name__, url_prefix="/admin")
        for endpoint in ("readings_enter", "readings_view", "readings_consumption", "muni_accounts", "billing_tariffs"):
            blueprint.add_url_rule("/fixture/" + endpoint, endpoint=endpoint, view_func=lambda: "fixture")
        modules = {}
        for name in ("app", "app.admin", "app.extensions", "app.models", "app.models.auth", "app.program_billing", "app.admin.programs", "app.admin.programs.billing"):
            modules[name] = types.ModuleType(name)
            modules[name].__path__ = []
        modules["app.admin"].admin_bp = blueprint
        modules["app.extensions"].db = self.database
        modules["app.models.auth"].User = User
        modules["app.models.auth"].ApprovedAdmin = ApprovedAdmin
        metadata = Mock()
        metadata.name = "Property Management"
        metadata.slug = "billing"
        catalog = Mock()
        catalog.filter_by.return_value.first.return_value = metadata
        modules["app.models.auth"].AuthSubject = types.SimpleNamespace(query=catalog)
        module_patch = patch.dict(sys.modules, modules)
        module_patch.start()
        self.addCleanup(module_patch.stop)
        self.storage = load("app.program_billing.lease_documents", ROOT / "app/program_billing/lease_documents.py")
        self.routes = load("app.admin.programs.billing.lease_management", ROOT / "app/admin/programs/billing/lease_management.py")
        self.programs = load("pm_program_routes_isolated", ROOT / "app/admin/programs/routes.py")
        self.app = Flask("pm_lease_test", root_path=str(ROOT / "app"),
            instance_path=str(Path(self.temp.name) / "instance"), static_folder="static")
        self.app.config.update(TESTING=True, SECRET_KEY="isolated-test", PM_LEASE_DOCUMENT_ROOT=str(self.private))
        self.app.jinja_loader = DictLoader({
            "layout.html": "{% block content %}{% endblock %}",
            "partials/flash_messages.html": "",
        })
        login = LoginManager(self.app)
        @login.user_loader
        def identity(identifier):
            return Identity(int(identifier))
        self.app.register_blueprint(blueprint)
        self.client = self.app.test_client()

    def authorize(self, statement):
        self.last_statement = str(statement.compile(compile_kwargs={"literal_binds": True}))
        identifier = current_user.id
        allowed = self.user_active.get(identifier, False) and self.grants.get(identifier, False)
        return types.SimpleNamespace(scalar_one_or_none=lambda: identifier if allowed else None)

    def sign_in(self, identifier, **flags):
        with self.client.session_transaction() as session:
            session["_user_id"] = str(identifier)
            session["_fresh"] = True
            session.update(flags)

    def paths(self):
        return ["/admin/billing/leases", "/admin/billing/leases/preview", "/admin/billing/leases/word"]

    def test_anonymous_denied_before_storage_or_database(self):
        with patch.object(self.routes, "read_review_pair", side_effect=AssertionError("must not read")):
            for path in self.paths():
                response = self.client.get(path)
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.database.session.execute.assert_not_called()

    def test_authenticated_regular_user_manager_and_forged_flags_denied(self):
        for identifier in (2, 3):
            self.sign_in(identifier, is_admin=True, role="admin", access_level="admin")
            with patch.object(self.routes, "read_review_pair", side_effect=AssertionError("must not read")):
                for path in self.paths():
                    self.assertEqual(self.client.get(path).status_code, 403)

    def test_inactive_user_denied_even_with_active_admin_grant(self):
        self.sign_in(4)
        for path in self.paths():
            self.assertEqual(self.client.get(path).status_code, 403)

    def test_revoked_admin_denied_on_every_endpoint(self):
        self.sign_in(1)
        self.assertEqual(self.client.get(self.paths()[0]).status_code, 200)
        self.grants[1] = False
        for path in self.paths():
            self.assertEqual(self.client.get(path).status_code, 403)

    def test_authorization_query_checks_current_database_identity_and_active_grant(self):
        self.sign_in(1)
        self.client.get(self.paths()[0])
        sql = self.last_statement
        self.assertIn('JOIN auth_approved_admin', sql)
        self.assertIn('lower(auth_approved_admin.email) = lower("user".email)', sql)
        self.assertIn('"user".id = 1', sql)
        self.assertIn('"user".is_active = 1', sql)
        self.assertIn("CAST(auth_approved_admin.active AS VARCHAR) IN ('true', '1')", sql)

    def test_database_failure_fails_closed(self):
        self.sign_in(1)
        self.database.session.execute.side_effect = SQLAlchemyError("fixture database unavailable")
        with patch.object(self.routes, "read_review_pair", side_effect=AssertionError("must not read")):
            for path in self.paths():
                self.assertEqual(self.client.get(path).status_code, 503)

    def test_admin_page_status_and_navigation_render_with_pm_scoped_loader(self):
        self.sign_in(1)
        response = self.client.get(self.paths()[0])
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("DRAFT — NOT FOR SIGNATURE OR ISSUANCE", html)
        self.assertIn("Lease_Review_Draft_R01.docx", html)
        self.assertIn("View Lease", html)
        self.assertIn("Download Word", html)
        self.assertIn('href="/admin/billing/"', html)
        self.assertIn('href="/admin/billing/leases/preview"', html)
        self.assertIn('href="/admin/billing/leases/word"', html)

    def test_pdf_is_exact_verified_word_preview_not_historical_lease(self):
        self.sign_in(1)
        response = self.client.get(self.paths()[1])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, self.original[self.storage.PDF_NAME])
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertTrue(response.headers["Content-Disposition"].startswith("inline;"))
        import fitz
        pdf = fitz.open(stream=response.data, filetype="pdf")
        self.assertEqual(len(pdf), 33)
        self.assertIn("Lease review draft", pdf[0].get_text())
        self.assertIn("Part 3 Schedule 2", " ".join(page.get_text() for page in pdf))

    def test_word_download_is_byte_identical_and_all_responses_private(self):
        self.sign_in(1)
        response = self.client.get(self.paths()[2])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, self.original[self.storage.WORD_NAME])
        self.assertIn("attachment;", response.headers["Content-Disposition"])
        self.assertEqual(response.mimetype, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        for path in self.paths():
            response = self.client.get(path)
            self.assertEqual(response.headers["Cache-Control"], "private, no-store")
            self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
            self.assertEqual(response.headers["Content-Security-Policy"], "frame-ancestors 'self'")
            self.assertNotIn("ETag", response.headers)

    def test_missing_files_show_unavailable_and_no_document_links(self):
        self.sign_in(1)
        (self.private / "R01" / self.storage.PDF_NAME).unlink()
        response = self.client.get(self.paths()[0])
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("View Lease", response.get_data(as_text=True))
        self.assertIn("temporarily unavailable", response.get_data(as_text=True))
        for path in self.paths()[1:]:
            self.assertEqual(self.client.get(path).status_code, 503)

    def test_tampering_either_document_blocks_entire_pair(self):
        self.sign_in(1)
        for name in (self.storage.WORD_NAME, self.storage.PDF_NAME):
            path = self.private / "R01" / name
            original = path.read_bytes()
            path.write_bytes(original + b"changed")
            for url in self.paths():
                self.assertEqual(self.client.get(url).status_code, 503)
            path.write_bytes(original)

    def test_historical_pdf_cannot_be_substituted(self):
        self.sign_in(1)
        historical = ROOT / "app/static/pdf/pm/Lease Graham Curtis Clack.docx.pdf"
        shutil.copyfile(historical, self.private / "R01" / self.storage.PDF_NAME)
        for path in self.paths():
            self.assertEqual(self.client.get(path).status_code, 503)

    def test_public_storage_configuration_fails_closed(self):
        self.sign_in(1)
        self.app.config["PM_LEASE_DOCUMENT_ROOT"] = str(ROOT / "app/static/pdf/pm")
        for path in self.paths():
            self.assertEqual(self.client.get(path).status_code, 503)

    def test_private_documents_have_no_static_or_arbitrary_filename_route(self):
        self.sign_in(1)
        for path in ["/static/pm_lease_documents/R01/Lease_Review_Draft_R01.docx",
                     "/static/pdf/pm/Lease_Review_Draft_R01.pdf",
                     "/admin/billing/leases/word/other.docx", "/admin/billing/leases/preview/../word"]:
            self.assertEqual(self.client.get(path).status_code, 404)
        for path in self.paths():
            self.assertEqual(self.client.post(path).status_code, 405)

    def test_real_pm_dashboard_links_to_viewer_and_back_without_startup(self):
        self.sign_in(1)
        response = self.client.get("/admin/billing/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('href="/admin/billing/leases"', html)
        self.assertIn('Open Lease Management', html)
        self.assertEqual(html.count('Under Development'), 2)
        page = self.client.get('/admin/billing/leases').get_data(as_text=True)
        self.assertIn('href="/admin/billing/"', page)

    def test_admin_tile_only_and_other_modules_remain_inactive(self):
        env = Environment(loader=DictLoader({"tiles": (ROOT / "templates/program_billing/dashboard_tiles.html").read_text(encoding="utf-8")}))
        env.globals['url_for'] = lambda endpoint, **kwargs: '/fixture/' + endpoint
        for admin in (True, False):
            html = env.get_template("tiles").render(pm_admin_dashboard=admin)
            self.assertEqual('Open Lease Management' in html, admin)
            self.assertEqual(html.count('Under Development'), 2)
            self.assertNotIn('muni_recon_statement', html)
            self.assertNotIn('tenant_accounts', html)


class PageLinks(HTMLParser):
    """Read actual rendered links rather than inventing navigation URLs."""
    def __init__(self, html):
        super().__init__()
        self.links = {}
        self.current = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.current = [dict(attrs).get("href"), ""]

    def handle_data(self, data):
        if self.current is not None:
            self.current[1] += data

    def handle_endtag(self, tag):
        if tag == "a" and self.current is not None:
            href, label = self.current
            self.links[label.strip()] = href
            self.current = None


class RealTemplateLeaseViewerTests(LeaseViewerTests):
    """Repeat all 16 checks with the real layout, plus navigation integration.

    Only selected source modules are loaded under synthetic app packages.
    No factory, configuration module, dotenv, database or network is needed.
    Navbar/METSOA fixture destinations do not test those production workflows.
    """
    def setUp(self):
        # Fail loudly if future harness changes try to load settings or connect.
        from contextlib import ExitStack
        import socket
        import dotenv
        self.safety = ExitStack()
        self.addCleanup(self.safety.close)
        for target in ("load_dotenv", "dotenv_values", "find_dotenv"):
            self.safety.enter_context(patch.object(dotenv, target,
                side_effect=AssertionError("Environment-file loading forbidden")))
        self.safety.enter_context(patch.object(socket.socket, "connect",
            side_effect=AssertionError("Network connections forbidden")))
        self.safety.enter_context(patch.object(socket.socket, "connect_ex",
            side_effect=AssertionError("Network connections forbidden")))
        self.safety.enter_context(patch("sqlalchemy.engine.Engine.connect",
            side_effect=AssertionError("Database connections forbidden")))
        super().setUp()
        from datetime import datetime, timezone
        from flask_wtf.csrf import CSRFProtect
        from jinja2 import FileSystemLoader, StrictUndefined
        self.app.jinja_loader = FileSystemLoader(str(ROOT / "templates"))
        self.app.jinja_env.undefined = StrictUndefined
        CSRFProtect(self.app)
        self.app.config["WTF_CSRF_ENABLED"] = True
        self.app.config["MAIL_SUPPRESS_SEND"] = True

        def synthetic_identity(identifier):
            person = Identity(int(identifier))
            person.name = "Synthetic administrator" if person.id == 1 else "Synthetic user"
            person.email = "synthetic-" + str(person.id) + "@example.invalid"
            return person
        self.app.login_manager.user_loader(synthetic_identity)
        self.app.add_url_rule("/fixture/welcome", endpoint="public_bp.welcome", view_func=lambda: "fixture")
        self.app.add_url_rule("/fixture/contact", endpoint="public_bp.contact", view_func=lambda: "fixture")
        self.app.add_url_rule("/fixture/login", endpoint="auth_bp.login", view_func=lambda: "fixture")
        self.app.add_url_rule("/fixture/logout", endpoint="auth_bp.logout", view_func=lambda: "fixture", methods=["POST"])
        self.app.add_url_rule("/fixture/admin", endpoint="admin_bp.loss_dashboard", view_func=lambda: "fixture")
        @self.app.context_processor
        def synthetic_layout_context():
            return dict(now=lambda: datetime.now(timezone.utc),
                nav_live_programs=[types.SimpleNamespace(slug="billing", name="Property Management")],
                nav_coming_soon=[], hide_navbar=False, breadcrumbs=[], nav_back_url=None,
                home_auditor=False)

    def test_real_layout_navbar_warning_and_flash_messages(self):
        self.sign_in(1, is_admin=True)
        with self.client.session_transaction() as session:
            session["_flashes"] = [("warning", "Synthetic review reminder")]
        response = self.client.get(self.paths()[0])
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('<!DOCTYPE html>', html)
        self.assertIn('<title>Lease Management | Property Management</title>', html)
        self.assertIn('Live Programs', html)
        self.assertIn('Hi, Synthetic administrator', html)
        self.assertIn('name="csrf_token"', html)
        self.assertIn('Synthetic review reminder', html)
        self.assertEqual(html.count(self.routes.STATUS), 1)
        self.assertIn('border-amber-300 bg-amber-50 p-3 text-sm font-bold text-amber-900', html)
        self.assertIn('<main id="app-main"', html)
        self.assertFalse(hasattr(sys.modules["app"], "create_app"))

    def test_follow_dashboard_lease_preview_word_and_back_links(self):
        self.sign_in(1)
        dashboard = self.client.get('/admin/billing/')
        self.assertEqual(dashboard.status_code, 200)
        links = PageLinks(dashboard.get_data(as_text=True)).links
        page = self.client.get(links['Open Lease Management'])
        self.assertEqual(page.status_code, 200)
        lease_links = PageLinks(page.get_data(as_text=True)).links
        preview = self.client.get(lease_links['View Lease'])
        self.assertEqual(preview.status_code, 200)
        self.assertEqual(preview.data, self.original[self.storage.PDF_NAME])
        import fitz
        with fitz.open(stream=preview.data, filetype='pdf') as pdf:
            self.assertEqual(len(pdf), 33)
            self.assertTrue(all(page.get_text().strip() for page in pdf))
        word = self.client.get(lease_links['Download Word'])
        self.assertEqual(word.status_code, 200)
        self.assertEqual(word.data, self.original[self.storage.WORD_NAME])
        from io import BytesIO
        from zipfile import ZipFile
        with ZipFile(BytesIO(word.data)) as package:
            self.assertIn('word/document.xml', package.namelist())
            self.assertIsNone(package.testzip())
        back = self.client.get(lease_links['Back to Property Management'])
        self.assertEqual(back.status_code, 200)
        self.assertIn('<title>Property Management</title>', back.get_data(as_text=True))

    def test_real_dashboard_metsoa_links_and_disabled_modules(self):
        self.sign_in(1)
        html = self.client.get('/admin/billing/').get_data(as_text=True)
        links = PageLinks(html).links
        for label in ('Enter Readings', 'View Readings', 'Compute Consumption', 'Export / Print', 'Tariffs & Rates'):
            self.assertEqual(self.client.get(links[label]).status_code, 200)
            self.assertTrue(links[label].startswith('/admin/fixture/'))
        self.assertEqual(html.count('Under Development'), 2)
        self.assertNotIn('tenant_accounts', html)
        self.assertNotIn('muni_recon_statement', html)

    def test_revocation_denies_links_obtained_from_real_page(self):
        self.sign_in(1)
        html = self.client.get(self.paths()[0]).get_data(as_text=True)
        links = PageLinks(html).links
        self.grants[1] = False
        for label in ('View Lease', 'Download Word'):
            self.assertEqual(self.client.get(links[label]).status_code, 403)
        self.assertEqual(self.client.get(self.paths()[0]).status_code, 403)


if __name__ == "__main__":
    unittest.main(verbosity=2)
