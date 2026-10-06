"""Real factory on verified local PostgreSQL, read-only; no startup repairs."""
import sys
from pathlib import Path
import unittest
from unittest.mock import patch
from dotenv import dotenv_values
from sqlalchemy import event, text
from sqlalchemy.engine import Engine, make_url
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class OperatorStartup(unittest.TestCase):
    def test_real_factory_readonly_and_no_mutation_attempts(self):
        url = make_url(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        if (url.get_backend_name() != "postgresql" or url.host not in
                ("localhost", "127.0.0.1", "::1") or url.database != "ait_local_db" or url.query):
            raise RuntimeError("Requires verified localhost ait_local_db without URL overrides.")
        attempted = []
        def guard(conn, cursor, statement, parameters, context, executemany):
            verb = statement.strip().split()[0].upper()
            if verb not in {"SELECT", "SHOW", "SET"}:
                attempted.append(statement)
                raise AssertionError("Unexpected startup SQL mutation")
        event.listen(Engine, "before_cursor_execute", guard)
        self.addCleanup(event.remove, Engine, "before_cursor_execute", guard)
        from app import create_app
        from app.extensions import db
        with patch("flask_migrate.upgrade", side_effect=AssertionError("Migration attempted")) as upgrade:
            app = create_app({"TESTING": True, "SQLALCHEMY_DATABASE_URI": url,
                "SQLALCHEMY_ENGINE_OPTIONS": {"connect_args": {
                    "options": "-c default_transaction_read_only=on"}}}, operator_mode=True)
        upgrade.assert_not_called()
        with app.app_context():
            self.assertEqual(db.session.execute(text("SHOW transaction_read_only")).scalar_one(), "on")
            db.session.rollback()
            db.engine.dispose()
        self.assertEqual(attempted, [])
        self.assertTrue(app.config["AIT_OPERATOR_MODE"])
        group = app.cli.commands["home_sace_bp"]
        self.assertIn("publish-document", group.commands)
        self.assertIn("publish-endorsement-documents", group.commands)
        # Listing operators must also avoid startup writes.
        result = app.test_cli_runner().invoke(args=["home_sace_bp", "--help"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(attempted, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
