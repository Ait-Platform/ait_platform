"""Diagnostic mail route regression without app startup, database or SMTP."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock

from flask import Blueprint, Flask

ROOT = Path(__file__).resolve().parents[1]


class DebugSendmailRemovedTests(unittest.TestCase):
    def test_unauthenticated_requests_cannot_send_mail(self):
        tree = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8-sig"))
        # Load any production handler registered at this URL. Reintroducing the
        # original route makes this test send to the mock and return 200.
        handlers = []
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and decorator.args
                        and isinstance(decorator.args[0], ast.Constant)
                        and decorator.args[0].value == "/debug/sendmail"):
                    handlers.append(node)
                    break
        app = Flask("removed-mail-diagnostic")
        app.config["TESTING"] = True
        blueprint = Blueprint("auth_bp", __name__, url_prefix="/")
        send_email = Mock(return_value=True)
        exec(compile(ast.Module(body=handlers, type_ignores=[]),
                     "<production-diagnostic-routes>", "exec"),
             {"auth_bp": blueprint, "send_email": send_email})
        app.register_blueprint(blueprint)
        client = app.test_client()
        for method in ("GET", "HEAD", "POST", "OPTIONS"):
            with self.subTest(method=method):
                self.assertEqual(client.open("/debug/sendmail", method=method).status_code, 404)
        send_email.assert_not_called()
        self.assertNotIn("auth_bp._debug_sendmail", app.view_functions)


if __name__ == "__main__":
    unittest.main()
