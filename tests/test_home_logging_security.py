"""Exercise the production trace hooks/filter without app startup or database writes."""
import ast
import importlib.util
import io
import logging
from pathlib import Path
import unittest
import uuid
from urllib.parse import quote

from flask import Flask, g, redirect, request, session

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("logging_security", ROOT / "app/logging_security.py")
security = importlib.util.module_from_spec(spec)
spec.loader.exec_module(security)
NONCE = "distinctive-HOME-nonce-72814"
SECRET = "distinctive-form-secret-98271"


class HomeLoggingSecurityTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask("home_logging_" + uuid.uuid4().hex)
        self.app.secret_key = "test-only"
        self.app.logger.setLevel(logging.INFO)
        self.app.logger.propagate = False
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.app.logger.handlers = [self.handler]
        self.app.logger.addFilter(security.RequestSecretFilter())
        tree = ast.parse((ROOT / "app/__init__.py").read_text(encoding="utf-8"))
        factory = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                       and node.name == "create_app")
        hooks = [node for node in factory.body if isinstance(node, ast.FunctionDef)
                 and node.name in {"_trace_in", "_trace_out"}]
        self.assertEqual(len(hooks), 2)
        exec(compile(ast.Module(body=hooks, type_ignores=[]), "<production-traces>", "exec"),
             dict(app=self.app, request=request, g=g, uuid=uuid))

    def tearDown(self):
        self.handler.close()

    def assert_safe(self):
        output = self.stream.getvalue()
        for secret in (NONCE, SECRET, quote(SECRET, safe="")):
            self.assertNotIn(secret, output)
        self.assertIn("[REDACTED]", output)
        return output

    def test_provisioning_and_authentication_request_and_redirect_traces(self):
        endpoints = {"/sace/home/provisioning": "home_sace_bp.provision",
                     "/sace/home/authenticate": "home_sace_bp.authenticate",
                     "/sace/home/join": "home_sace_bp.join",
                     "/sace/home/pledge": "home_sace_bp.pledge",
                     "/login": "auth_bp.login", "/register": "auth_bp.register",
                     "/start_registration": "auth_bp.start_registration",
                     "/register/decision": "auth_bp.register_decision",
                     "/sace/home/": "home_sace_bp.entry"}
        target = "/sace/home/provisioning?journey=" + NONCE + "&view=ordinary"
        for path, endpoint in endpoints.items():
            self.app.add_url_rule(path, endpoint, lambda: redirect(target), methods=["GET", "POST"])
        client = self.app.test_client()
        for path in endpoints:
            for method in ("GET", "POST"):
                with self.subTest(path=path, method=method):
                    response = client.open(path, method=method,
                        query_string={"journey": NONCE, "next": target, "view": "ordinary"},
                        data={"password": SECRET, "csrf_token": SECRET, "signature": SECRET,
                              "token": SECRET, "code": SECRET, "journey": NONCE})
                    self.assertEqual(response.location, target)
                    self.assertEqual(response.status_code, 302)
                    self.assert_safe()
        self.assertIn("ordinary", self.stream.getvalue())

    def test_new_redirect_nonce_and_nested_encoded_next_are_redacted(self):
        target = "/sace/home/provisioning?journey=" + NONCE + "&view=ordinary"
        self.app.add_url_rule("/fresh", view_func=lambda: redirect(target))
        self.assertEqual(self.app.test_client().get("/fresh").location, target)
        self.app.logger.info("redirect to /register?next=%s", quote(quote(target, safe=""), safe=""))
        self.assert_safe()

    def test_errors_before_trace_and_exception_tracebacks(self):
        with self.app.test_request_context("/login", query_string={"next":
                "/sace/home/provisioning?journey=" + NONCE},
                method="POST", data={"password": SECRET}):
            self.app.logger.warning("rejected value=%s", SECRET)
            try:
                raise ValueError("provisioning failed " + NONCE + " " + SECRET)
            except ValueError:
                self.app.logger.exception("authentication failed %s", SECRET)
        output = self.assert_safe()
        self.assertIn("Traceback", output)
        self.assertIn("ValueError", output)
        self.assertIn("authentication failed", output)

    def test_session_nonce_and_duplicate_secret_values(self):
        from werkzeug.datastructures import MultiDict
        with self.app.test_request_context("/login", method="POST",
                data=MultiDict([("token", SECRET), ("token", NONCE)])):
            session["sace_home_provisioning_context"] = {"nonce": NONCE}
            self.app.logger.error("values=%s %s", SECRET, NONCE)
        self.assert_safe()

    def test_flask_unhandled_exception_and_encoded_secret(self):
        def fail():
            raise RuntimeError("failed with " + request.form["password"])

        self.app.add_url_rule("/login", "auth_bp.login", fail, methods=["POST"])
        response = self.app.test_client().post("/login", data={"password": SECRET})
        self.assertEqual(response.status_code, 500)
        output = self.assert_safe()
        self.assertIn("RuntimeError", output)
        self.assertIn("500", output)
        punctuation_secret = "sensitive'\\value+with spaces/&"
        with self.app.test_request_context("/login", method="POST", data={"password": punctuation_secret}):
            self.app.logger.error("form=%s encoded=%s", dict(request.form), quote(punctuation_secret, safe=""))
        for value in (punctuation_secret, repr(punctuation_secret)[1:-1], quote(punctuation_secret, safe="")):
            self.assertNotIn(value, self.stream.getvalue())

    def test_ordinary_request_logging_retains_args_form_and_location(self):
        self.app.add_url_rule("/ordinary", view_func=lambda: redirect("/results?page=2"), methods=["POST"])
        response = self.app.test_client().post("/ordinary?search=reading", data={"comment": "useful-comment"})
        self.assertEqual(response.location, "/results?page=2")
        output = self.stream.getvalue()
        for value in ("reading", "useful-comment", "/ordinary", "/results?page=2", "302"):
            self.assertIn(value, output)
        self.assertNotIn("[REDACTED]", output)

    def test_authentication_token_in_route_path_is_redacted(self):
        self.app.add_url_rule("/reset/<token>", "auth_bp.reset", lambda token: "reset")
        self.assertEqual(self.app.test_client().get("/reset/" + NONCE).status_code, 200)
        self.assert_safe()


if __name__ == "__main__":
    unittest.main(verbosity=2)
