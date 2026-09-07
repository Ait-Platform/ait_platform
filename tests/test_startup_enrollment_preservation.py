"""Run directly with python -B tests/test_startup_enrollment_preservation.py.

Executes the actual first startup context and trial-grant branch extracted by AST,
with in-memory persistence. Never imports the app, runs its factory, or connects
to a database. The repository's database-creating pytest fixture is not used.
"""
import ast
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
READING_SQL = "UPDATE auth_subject SET paid_days = NULL WHERE slug = 'reading'"
SPV_SQL = "ALTER TABLE spv_participations ADD COLUMN pseudonym VARCHAR(100)"


class MemorySession:
    def __init__(self, enrollment, fail_reading=False):
        self.enrollment = enrollment
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_reading = fail_reading

    def execute(self, statement):
        statement = str(statement)
        self.executed.append(statement)
        normalized = " ".join(statement.split()).lower()
        # Model the deleted UPDATE so the tests would detect its return.
        if normalized.startswith("update user_enrollment"):
            if self.enrollment.status == "active":
                self.enrollment.status = "trial"
                self.enrollment.trial_end = datetime(2030, 1, 1) + timedelta(days=15)
        if self.fail_reading and statement == READING_SQL:
            raise RuntimeError("Synthetic neighbouring maintenance failure")

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def get(self, model, identifier):
        return self.enrollment if identifier == self.enrollment.id else None


def startup_context():
    module = ast.parse((ROOT / "app/__init__.py").read_text(encoding="utf-8-sig"))
    factory = next(n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "create_app")
    return next(n for n in factory.body if isinstance(n, ast.With))


def execute_startup(session):
    block = deepcopy(startup_context())
    code = compile(ast.fix_missing_locations(ast.Module(body=[block], type_ignores=[])),
                   "isolated-startup-context", "exec")
    exec(code, {"app": SimpleNamespace(app_context=nullcontext),
                "db": SimpleNamespace(session=session), "text": lambda value: value})


class StartupEnrollmentPreservationTests(unittest.TestCase):
    def setUp(self):
        # Synthetic record only; ID intentionally resembles the observed case.
        self.enrollment = SimpleNamespace(id=964, status="active",
            trial_end=datetime(2026, 9, 13, 3, 49, 47), trial_count=1,
            subject_id=41, enroll_policy="auto_enroll")
        self.before = deepcopy(vars(self.enrollment))
        self.session = MemorySession(self.enrollment)

    def test_startup_preserves_active_auto_enrollment(self):
        execute_startup(self.session)
        self.assertEqual(vars(self.enrollment), self.before)
        self.assertFalse(any("update user_enrollment" in " ".join(s.split()).lower()
                             for s in self.session.executed))

    def test_repeated_worker_initialization_preserves_original_expiry(self):
        for _ in range(3):
            execute_startup(self.session)
            self.assertEqual(vars(self.enrollment), self.before)

    def test_neighbouring_startup_statements_and_commits_are_preserved(self):
        execute_startup(self.session)
        index = self.session.executed.index(READING_SQL)
        self.assertEqual(self.session.executed[index + 1], SPV_SQL)
        self.assertEqual(self.session.commits, len(self.session.executed))
        self.assertEqual(self.session.rollbacks, 0)

    def test_neighbouring_failure_still_rolls_back_and_continues(self):
        session = MemorySession(self.enrollment, fail_reading=True)
        execute_startup(session)
        self.assertEqual(session.rollbacks, 1)
        self.assertIn(SPV_SQL, session.executed)
        self.assertEqual(session.commits, len(session.executed) - 1)
        self.assertEqual(vars(self.enrollment), self.before)

    def test_explicit_trial_grant_still_uses_subject_configuration(self):
        module = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8-sig"))
        branch = next(n for n in ast.walk(module) if isinstance(n, ast.If)
                      and ast.unparse(n.test) == "subj_obj.trial_days and float(subj_obj.trial_days) > 0")
        wrapper = ast.parse("def grant(): pass")
        wrapper.body[0].body = [deepcopy(branch), ast.Return(value=ast.Constant(None))]
        code = compile(ast.fix_missing_locations(wrapper), "isolated-explicit-trial-grant", "exec")
        for days in (7, 30):
            with self.subTest(trial_days=days):
                session = MemorySession(self.enrollment)
                context = {
                    "subj_obj": SimpleNamespace(trial_days=days, slug="example"),
                    "current_app": SimpleNamespace(config={}),
                    "request": SimpleNamespace(host="production.example"),
                    "db": SimpleNamespace(session=session), "UserEnrollment": object,
                    "enrollment_id": 964, "datetime": datetime, "timedelta": timedelta,
                    "redirect": lambda value: value, "url_for": lambda endpoint: endpoint,
                }
                before = datetime.utcnow()
                exec(code, context)
                self.assertEqual(context["grant"](), "auth_bp.bridge_dashboard")
                after = datetime.utcnow()
                self.assertEqual(self.enrollment.status, "active")
                self.assertGreaterEqual(self.enrollment.trial_end, before + timedelta(days=days))
                self.assertLessEqual(self.enrollment.trial_end, after + timedelta(days=days))
                self.assertEqual(session.commits, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
