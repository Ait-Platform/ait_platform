"""Exercise the production request logger without starting the platform or DB."""
import ast
import importlib.util
import io
import logging
from pathlib import Path
import unittest
import uuid

from flask import Flask, g, request
from werkzeug.datastructures import MultiDict

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "request_logging_helper", ROOT / "app/utils/request_logging.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


class RequestLogRedaction(unittest.TestCase):
    def test_sensitive_aliases_redacted_without_changing_input(self):
        names = (
            "password", "confirm_password", "password_confirmation", "new-password",
            "old_password", "tutor_password", "PASSWORD", "passwd", "pwd",
            "csrf_token", "csrf-token", "token", "access_token", "refresh_token",
            "code", "access_code", "invitation_code", "journey", "handover",
            "voucher", "voucher_code", "secret", "client_secret", "password_hash",
            "clearing_house_api_key", "API_KEY", "access_key_id", "private_key",
            "authorization", "session_id", "otp", "pin", "request_key",
        )
        fields = {name: "dummy-sensitive-" + str(i) for i, name in enumerate(names)}
        original = fields.copy()
        result = helper.redacted_fields(fields)
        self.assertEqual(fields, original)
        self.assertEqual(set(result), set(fields))
        self.assertTrue(all(value == "[REDACTED]" for value in result.values()))

    def test_safe_fields_and_duplicate_sensitive_values(self):
        fields = MultiDict([
            ("password", "dummy-first"), ("password", "dummy-second"),
            ("email", "test@example.test"), ("action", "email"),
            ("country_code", "ZA"), ("page", "2"),
        ])
        result = helper.redacted_fields(fields)
        self.assertEqual(result, {
            "password": "[REDACTED]", "email": "test@example.test",
            "action": "email", "country_code": "ZA", "page": "2",
        })
        self.assertEqual(fields.getlist("password"), ["dummy-first", "dummy-second"])

    def test_actual_incoming_logger_output_contains_no_sensitive_values(self):
        # Extract the actual hook, avoiding create_app's startup database work.
        tree = ast.parse((ROOT / "app/__init__.py").read_text(encoding="utf-8"))
        hook = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef) and node.name == "_trace_in")
        hook.decorator_list = []
        app = Flask("request_redaction_test")
        namespace = dict(app=app, g=g, request=request, uuid=uuid,
                         redacted_fields=helper.redacted_fields)
        exec(compile(ast.Module(body=[hook], type_ignores=[]),
                     "app/__init__.py", "exec"), namespace)
        app.before_request(namespace["_trace_in"])
        app.add_url_rule("/diagnostic", view_func=lambda: "OK", methods=["POST"])
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        previous_level = app.logger.level
        app.logger.addHandler(handler)
        app.logger.setLevel(logging.INFO)
        try:
            response = app.test_client().post(
                "/diagnostic?token=dummy-query-token&page=2",
                data={"password": "dummy-password", "csrf_token": "dummy-csrf",
                      "code": "dummy-access-code", "journey": "dummy-journey",
                      "clearing_house_api_key": "dummy-api-key",
                      "email": "safe@example.test", "action": "view"})
            self.assertEqual(response.status_code, 200)
        finally:
            app.logger.removeHandler(handler)
            app.logger.setLevel(previous_level)
        logged = output.getvalue()
        for secret in ("dummy-query-token", "dummy-password", "dummy-csrf",
                       "dummy-access-code", "dummy-journey", "dummy-api-key"):
            self.assertNotIn(secret, logged)
        for safe in ("safe@example.test", "view", "page", "2", "POST", "/diagnostic"):
            self.assertIn(safe, logged)
        self.assertIn("[REDACTED]", logged)


if __name__ == "__main__":
    unittest.main()
