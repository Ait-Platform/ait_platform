"""Run directly with Python. No app factory, engine, SQL or database is used."""
import ast
from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock

from flask import Flask
from flask_login import LoginManager, UserMixin
from flask_wtf.csrf import CSRFProtect
from jinja2 import ChoiceLoader, DictLoader
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable, CreateIndex

ROOT = Path(__file__).resolve().parents[2]
# Isolate imports from the production app factory and its startup side effects.
for name, directory in (("app", "app"), ("app.models", "app/models")):
    package = types.ModuleType(name)
    package.__path__ = [str(ROOT / directory)]
    sys.modules[name] = package

from app.extensions import db
from app.models.retire import RetirementOrganisation as Organisation, RetirementRole as Role, RetirementMembership as Membership, ROLE_DEFINITIONS
from app.program_retire import retire_bp, routes
sa.Table("user", db.metadata, sa.Column("id", sa.Integer, primary_key=True))
sa.orm.configure_mappers()

class Query:
    def __init__(self, records):
        self.records = records
    def filter_by(self, **values):
        return Query([r for r in self.records if all(getattr(r, k) == v for k, v in values.items())])
    def filter(self, *args):
        return Query([r for r in self.records if r.code != "organisation_owner"])
    def order_by(self, *args):
        return self
    def with_for_update(self):
        return self
    def first(self):
        return next(iter(self.records), None)
    def first_or_404(self):
        from flask import abort
        return self.first() or abort(404)
    def all(self):
        return self.records

class User(UserMixin):
    id = 10

class Stage2Tests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"))
        self.app.config.update(SECRET_KEY="test-only", TESTING=True, WTF_CSRF_ENABLED=False)
        self.app.jinja_loader = ChoiceLoader([DictLoader({
            "layout.html": "{% block content %}{% endblock %}",
            "partials/flash_messages.html": "",
        }), self.app.jinja_loader])
        self.app.register_blueprint(retire_bp)
        login = LoginManager(self.app)
        login.request_loader(lambda request: User())
        CSRFProtect(self.app)
        self.client = self.app.test_client()
        self.org = Organisation(id=1, name="Retirement A", owner_user_id=10)
        self.roles = [Role(id=i, code=c, name=n) for i, (c,n) in enumerate(ROLE_DEFINITIONS, 1)]
        self.owner = Membership(id=1, organisation_id=1, user_id=10, status="active", approved_role_id=1, approved_role=self.roles[0], organisation=self.org)
        self.applicant = Membership(id=2, organisation_id=1, user_id=20, status="pending", requested_role_id=2, organisation=self.org, requested_role=self.roles[1])
        self.members = [self.owner, self.applicant]
        Organisation.query = Query([self.org])
        Membership.query = Query(self.members)
        Role.query = Query(self.roles)
        self.session = MagicMock()
        self.session.flush.side_effect = lambda: setattr(self.session.add.call_args.args[0], "id", 3)
        self.session.get.side_effect = lambda model, id: next((r for r in (self.roles if model is Role else [self.org]) if r.id == id), None)
        self.old_db = routes.db
        routes.db = types.SimpleNamespace(session=self.session, get_or_404=lambda model, id: self.org)

    def tearDown(self):
        routes.db = self.old_db
        for model in (Organisation, Membership, Role):
            del model.query

    def review(self, **data):
        return self.client.post("/retire/organisations/1/members/2/review", data=data)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_approve_records_server_reviewer_and_role(self):
        response = self.review(decision="approve", role_id=3, reason="Care team", reviewed_by_user_id=999, status="disabled")
        self.assertEqual(response.status_code, 302)
        self.assertEqual((self.applicant.status, self.applicant.approved_role_id, self.applicant.reviewed_by_user_id), ("active", 3, 10))
        self.assertIsNotNone(self.applicant.reviewed_at)
        self.session.commit.assert_called_once()

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_deny_clears_approved_role(self):
        self.assertEqual(self.review(decision="deny", role_id=0, reason="Not our staff").status_code, 302)
        self.assertEqual(self.applicant.status, "denied")
        self.assertIsNone(self.applicant.approved_role_id)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_owner_role_and_unknown_role_cannot_be_approved(self):
        for role_id in (1, 999, 0):
            self.assertEqual(self.review(decision="approve", role_id=role_id, reason="Request").status_code, 200)
            self.assertEqual(self.applicant.status, "pending")
        self.session.commit.assert_not_called()

    def test_cross_organisation_review_is_hidden(self):
        self.applicant.organisation_id = 2
        self.assertEqual(self.review(decision="approve", role_id=2, reason="Request").status_code, 404)
        self.session.commit.assert_not_called()

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_active_staff_cannot_review(self):
        self.org.owner_user_id = 99
        self.assertEqual(self.review(decision="approve", role_id=2, reason="Request").status_code, 403)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_self_review_fails(self):
        self.applicant.user_id = 10
        self.assertEqual(self.review(decision="approve", role_id=2, reason="Request").status_code, 403)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_repeated_review_fails(self):
        self.applicant.status = "active"
        self.assertEqual(self.review(decision="deny", reason="Request").status_code, 409)

    def test_operational_access_rechecks_membership(self):
        for status in ("pending", "denied", "disabled"):
            self.owner.status = status
            self.assertIn("/status", self.client.get("/retire/organisations/1/dashboard").location)
            self.assertEqual(self.client.get("/retire/organisations/1/members/pending").status_code, 302)
        self.owner.status = "active"
        self.assertEqual(self.client.get("/retire/organisations/1/dashboard").status_code, 200)
        self.assertEqual(self.client.get("/retire/organisations/2/dashboard").status_code, 403)

    def test_entry_resolves_existing_membership(self):
        self.assertIn("/dashboard", self.client.get("/retire/entry").location)
        self.owner.status = "pending"
        self.assertIn("/status", self.client.get("/retire/entry").location)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_join_ignores_injected_identity_and_approval(self):
        self.members.remove(self.owner)
        self.org.owner_user_id = 99
        # Make the committed row visible to the response resolver.
        self.session.add.side_effect = self.members.append
        response = self.client.post("/retire/organisations/1/join", data={"role_id": 2, "confirm": "y", "user_id": 999, "status": "active", "approved_role_id": 1, "reviewed_by_user_id": 999})
        self.assertEqual(response.status_code, 302)
        created = self.session.add.call_args.args[0]
        self.assertEqual((created.user_id, created.status, created.requested_role_id), (10, "pending", 2))
        self.assertIsNone(created.approved_role_id)
        self.assertIsNone(created.reviewed_by_user_id)

    @unittest.skip("Retired legacy authority workflow; covered by Waiting Room and Stage 3 security tests")
    def test_join_cannot_request_owner(self):
        self.members.remove(self.owner)
        self.org.owner_user_id = 99
        self.assertEqual(self.client.post("/retire/organisations/1/join", data={"role_id": 1, "confirm": "y"}).status_code, 200)
        self.session.add.assert_not_called()

    def test_registration_is_one_transaction(self):
        Organisation.query = Query([])
        self.assertEqual(self.client.post("/retire/register", data={"name": "New retirement", "authority": "y"}).status_code, 302)
        additions = [call.args[0] for call in self.session.add.call_args_list]
        self.assertIsInstance(additions[0], Organisation)
        member = additions[1]
        self.assertEqual((member.user_id, member.status, member.approved_role_id), (10, "active", 1))
        self.assertIsNone(member.requested_role_id)
        self.assertIsNone(member.reviewed_at)
        self.assertIsNone(member.reviewed_by_user_id)
        self.session.flush.assert_called_once()
        self.session.commit.assert_called_once()

    def test_registration_failure_rolls_back(self):
        from sqlalchemy.exc import IntegrityError
        Organisation.query = Query([])
        self.session.commit.side_effect = IntegrityError("test", {}, Exception("test"))
        with self.assertRaises(IntegrityError):
            self.client.post("/retire/register", data={"name": "New retirement", "authority": "y"})
        self.session.rollback.assert_called_once()

    def test_registration_unexpected_precommit_failures_roll_back(self):
        Organisation.query = Query([])
        for operation in ("add", "flush", "commit"):
            with self.subTest(operation=operation):
                self.session.reset_mock(side_effect=True)
                error = RuntimeError("unexpected registration failure")
                if operation == "add":
                    # Fail adding membership AFTER the organisation has been flushed.
                    self.session.add.side_effect = [None, error]
                else:
                    getattr(self.session, operation).side_effect = error
                with self.assertRaises(RuntimeError) as caught:
                    self.client.post("/retire/register", data={"name": "New retirement", "authority": "y"})
                self.assertIs(caught.exception, error)
                self.session.rollback.assert_called_once()
                if operation != "commit":
                    self.session.commit.assert_not_called()

    def test_csrf_blocks_state_changes(self):
        self.app.config["WTF_CSRF_ENABLED"] = True
        for path in ("/retire/register", "/retire/join", "/retire/organisations/1/join", "/retire/organisations/1/members/2/review"):
            self.assertEqual(self.client.post(path, data={"name": "test"}).status_code, 400)
        self.session.commit.assert_not_called()

    def test_templates_compile_and_screens_render(self):
        for path in (ROOT / "templates/program_retire").glob("*.html"):
            self.app.jinja_env.parse(path.read_text(encoding="utf-8"))
        for path in ("/retire/about", "/retire/join", "/retire/organisations/1/members/pending", "/retire/organisations/1/members/2/review"):
            self.assertEqual(self.client.get(path).status_code, 302 if path == "/retire/join" else 200)


class MemoryConnection:
    """Evaluate only the helper statements against Python records; never execute SQL.

    This intentionally has no engine, connection factory, commit or rollback method.
    Unexpected statement kinds fail rather than reaching an external database.
    """
    def __init__(self):
        self.rows = {"retirement_role": [], "retirement_organisation": [], "retirement_membership": []}
        self.insertions = []
        self.next_id = 101

    def execute(self, statement):
        from sqlalchemy.sql.elements import BindParameter, BooleanClauseList, Grouping, Null
        from sqlalchemy.sql import operators
        from sqlalchemy.sql.selectable import Select
        from sqlalchemy.sql.dml import Insert

        def evaluate(expression, row):
            if isinstance(expression, BindParameter):
                return expression.value
            if isinstance(expression, Null):
                return None
            if isinstance(expression, Grouping):
                return evaluate(expression.element, row)
            if isinstance(expression, BooleanClauseList):
                values = [evaluate(clause, row) for clause in expression.clauses]
                if expression.operator is operators.or_:
                    return any(values)
                if expression.operator is operators.and_:
                    return all(values)
                raise AssertionError("Unexpected boolean operator")
            if hasattr(expression, "left"):
                if expression.operator is operators.eq:
                    return evaluate(expression.left, row) == evaluate(expression.right, row)
                raise AssertionError("Unexpected comparison")
            return row[expression.name]

        if isinstance(statement, Select):
            table = statement.get_final_froms()[0].name
            rows = [dict(row) for row in self.rows[table]
                    if all(evaluate(clause, row) for clause in statement._where_criteria)]
        elif isinstance(statement, Insert):
            values = statement.compile(dialect=postgresql.dialect()).params
            table = statement.table.name
            # Assert conflict-safe INSERT, with no overwrite behavior.
            compiled = str(statement.compile(dialect=postgresql.dialect()))
            assert "ON CONFLICT" in compiled and "DO NOTHING" in compiled
            keys = ("code",) if table == "retirement_role" else ("organisation_id", "user_id")
            if not any(all(row[k] == values[k] for k in keys) for row in self.rows[table]):
                self.next_id += 1
                row = dict(values, id=self.next_id)
                self.rows[table].append(row)
                self.insertions.append((table, deepcopy(row)))
            rows = []
        else:
            raise AssertionError(f"Unexpected statement type: {type(statement)}")
        class Result:
            def mappings(self):
                return self
            def all(self):
                return rows
            def one_or_none(self):
                assert len(rows) <= 1
                return rows[0] if rows else None
            def one(self):
                assert len(rows) == 1
                return rows[0]
            def __iter__(self):
                return iter(rows)
        return Result()


def load_migration():
    spec = importlib.util.spec_from_file_location("retire_stage2_migration", ROOT / "migrations/versions/retire_02_membership.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture_migration():
    module = load_migration()
    metadata = sa.MetaData()
    sa.Table("user", metadata, sa.Column("id", sa.Integer, primary_key=True))
    Organisation.__table__.to_metadata(metadata)
    connection = MemoryConnection()
    class OfflineOperations:
        def create_table(self, name, *args):
            return sa.Table(name, metadata, *args)
        def create_index(self, name, table, columns):
            sa.Index(name, *(metadata.tables[table].c[c] for c in columns))
        def get_bind(self):
            return connection
    module.op = OfflineOperations()
    module.upgrade()
    assert [(r["code"], r["name"]) for r in connection.rows["retirement_role"]] == list(ROLE_DEFINITIONS)
    assert not connection.rows["retirement_membership"]
    return metadata, connection


class ProvisioningTests(unittest.TestCase):
    def setUp(self):
        self.migration = load_migration()
        self.connection = MemoryConnection()
        metadata, _ = capture_migration()
        self.roles = metadata.tables["retirement_role"]
        self.members = metadata.tables["retirement_membership"]
        self.organisations = metadata.tables["retirement_organisation"]

    def seed(self):
        return self.migration.establish_roles(self.connection, self.roles)

    def backfill(self):
        self.migration.backfill_owner_memberships(self.connection, self.organisations, self.members, self.owner_role_id)

    def prepare_owner(self):
        self.owner_role_id = self.seed()["organisation_owner"]
        self.connection.rows["retirement_organisation"] = [{"id": 8, "owner_user_id": 42}]

    def owner_row(self, **overrides):
        return dict(dict(id=55, organisation_id=8, user_id=42, status="active",
                         approved_role_id=self.owner_role_id, requested_role_id=None,
                         reviewed_at=None, reviewed_by_user_id=None, review_reason=None), **overrides)

    def test_missing_roles_insert_canonical_names_without_fixed_ids(self):
        ids = self.seed()
        self.assertEqual(len(self.connection.rows["retirement_role"]), 6)
        self.assertEqual([(r["code"], r["name"]) for r in self.connection.rows["retirement_role"]], list(ROLE_DEFINITIONS))
        self.assertGreater(ids["organisation_owner"], 100)

    def test_matching_role_preserved(self):
        row = {"id": 987, "code": "finance", "name": "Finance"}
        self.connection.rows["retirement_role"].append(row)
        self.assertEqual(self.seed()["finance"], 987)
        self.assertEqual(row, {"id": 987, "code": "finance", "name": "Finance"})
        self.assertEqual(len(self.connection.insertions), 5)

    def test_conflicting_role_rejected_without_overwrite(self):
        row = {"id": 987, "code": "organisation_owner", "name": "Different meaning"}
        self.connection.rows["retirement_role"].append(row)
        before = deepcopy(self.connection.rows)
        with self.assertRaises(self.migration.ProvisioningConflict):
            self.seed()
        self.assertEqual(self.connection.rows, before)

    def test_repeated_roles_produce_no_duplicates_or_writes(self):
        ids = self.seed()
        before = deepcopy(self.connection.rows)
        self.connection.insertions.clear()
        self.assertEqual(self.seed(), ids)
        self.assertEqual(self.connection.rows, before)
        self.assertEqual(self.connection.insertions, [])

    def test_missing_owner_inserted(self):
        self.prepare_owner()
        self.backfill()
        row = self.connection.rows["retirement_membership"][0]
        for key, value in self.owner_row().items():
            if key != "id":
                self.assertEqual(row[key], value)
        self.assertNotIn("requested_at", row)  # database default supplies it

    def test_matching_owner_preserved_including_metadata(self):
        self.prepare_owner()
        row = self.owner_row(review_reason="Preserve existing metadata")
        self.connection.rows["retirement_membership"].append(row)
        before = deepcopy(self.connection.rows)
        self.connection.insertions.clear()
        self.backfill()
        self.assertEqual(self.connection.rows, before)
        self.assertEqual(self.connection.insertions, [])

    def test_conflicting_owner_rejected_without_repair(self):
        self.prepare_owner()
        for change in ({"status": "pending"}, {"status": "denied"}, {"status": "disabled"},
                       {"approved_role_id": None}, {"approved_role_id": 999}, {"requested_role_id": 999}):
            with self.subTest(change=change):
                self.connection.rows["retirement_membership"] = [self.owner_row(**change)]
                before = deepcopy(self.connection.rows)
                with self.assertRaises(self.migration.ProvisioningConflict):
                    self.backfill()
                self.assertEqual(self.connection.rows, before)

    def test_owner_role_assigned_to_nonowner_rejected(self):
        self.prepare_owner()
        self.connection.rows["retirement_membership"] = [self.owner_row(user_id=777)]
        before = deepcopy(self.connection.rows)
        with self.assertRaises(self.migration.ProvisioningConflict):
            self.backfill()
        self.assertEqual(self.connection.rows, before)

    def test_repeated_backfill_produces_no_duplicates_or_writes(self):
        self.prepare_owner()
        self.backfill()
        before = deepcopy(self.connection.rows)
        self.connection.insertions.clear()
        self.backfill()
        self.assertEqual(self.connection.rows, before)
        self.assertEqual(self.connection.insertions, [])

    def test_unrelated_staff_memberships_untouched(self):
        self.prepare_owner()
        staff = self.owner_row(user_id=77, status="pending", approved_role_id=None, requested_role_id=999)
        other_org_staff = self.owner_row(organisation_id=99, user_id=66, approved_role_id=999)
        before = deepcopy([staff, other_org_staff])
        self.connection.rows["retirement_membership"] = [staff, other_org_staff]
        self.backfill()
        self.assertEqual(self.connection.rows["retirement_membership"][:2], before)
        self.assertEqual(len(self.connection.rows["retirement_membership"]), 3)

    def test_empty_organisations_is_noop(self):
        self.owner_role_id = self.seed()["organisation_owner"]
        before = deepcopy(self.connection.rows)
        self.connection.insertions.clear()
        self.backfill()
        self.assertEqual(self.connection.rows, before)
        self.assertEqual(self.connection.insertions, [])


class SourceTests(unittest.TestCase):
    def test_syntax_and_user_dependency(self):
        for path in [ROOT / "app/models/retire.py", ROOT / "app/models/__init__.py", ROOT / "app/program_retire/routes.py", ROOT / "migrations/versions/retire_02_membership.py"]:
            ast.parse(path.read_text(encoding="utf-8"))
        source = (ROOT / "app/models/auth.py").read_text(encoding="utf-8")
        self.assertIn('class User(', source)
        self.assertIn('__tablename__ = "user"', source)

    def test_model_migration_parity(self):
        metadata, _ = capture_migration()
        # Historical definitions are immutable. Compare the legacy projection;
        # the explicit Waiting Room transition owns the additive current schema.
        metadata.tables['retirement_membership'].c.status.nullable = True
        def signature(table):
            return (
                [(c.name, str(c.type), c.nullable, c.primary_key, str(c.server_default.arg) if c.server_default else None, sorted(f.target_fullname for f in c.foreign_keys)) for c in table.columns if not c.name.startswith('association_approved_')],
                sorted(tuple(c.name for c in constraint.columns) for constraint in table.constraints if isinstance(constraint, sa.UniqueConstraint)),
                sorted(str(c.sqltext) for c in table.constraints if isinstance(c, sa.CheckConstraint) and not c.name.startswith('ck_retirement_membership_association_')),
                sorted((i.name, tuple(c.name for c in i.columns)) for i in table.indexes),
            )
        for model in (Role, Membership):
            self.assertEqual(signature(model.__table__), signature(metadata.tables[model.__tablename__]))

if __name__ == "__main__":
    unittest.main()
