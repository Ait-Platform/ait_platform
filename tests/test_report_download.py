"""Report token regressions without app startup or database mutation."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock
import flask
from itsdangerous import URLSafeSerializer, BadData


class ReportDownloadTests(unittest.TestCase):
    def setUp(self):
        self.app = flask.Flask(__name__)
        self.app.secret_key = "test-report-secret"
        self.builder = Mock(return_value={"learner_name": "Test"})
        self.pdf = Mock(return_value=b"%PDF-test")
        env = {name: getattr(flask, name) for name in
               ("current_app", "abort", "request", "flash", "redirect", "url_for")}
        env.update(URLSafeSerializer=URLSafeSerializer, BadData=BadData,
                   build_learner_report_ctx=self.builder,
                   render_template=Mock(return_value="<html>report</html>"),
                   html_to_pdf_bytes=self.pdf)
        path = Path(__file__).resolve().parents[1] / "app/reports/routes.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "download_report")
        fn.decorator_list = []
        exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), "exec"), env)
        self.app.add_url_rule("/download/<token>", view_func=env["download_report"])
        self.client = self.app.test_client()
        self.signer = URLSafeSerializer(self.app.secret_key, salt="pdf-report")

    def test_existing_loss_token_without_config_downloads(self):
        response = self.client.get("/download/" + self.signer.dumps({"run_id": 12, "user_id": 3}))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"%PDF-test")
        self.assertEqual(response.mimetype, "application/pdf")
        self.builder.assert_called_once_with(12, 3)

    def test_explicit_serializer_override_is_preserved(self):
        signer = URLSafeSerializer("override", salt="custom")
        self.app.config["REPORT_SERIALIZER"] = signer
        response = self.client.get("/download/" + signer.dumps({"run_id": 12, "user_id": 3}))
        self.assertEqual(response.status_code, 200)

    def test_invalid_signature_rejected_before_data_access(self):
        self.assertEqual(self.client.get("/download/not-a-valid-token").status_code, 403)
        self.builder.assert_not_called()

    def test_signed_malformed_payload_rejected(self):
        for payload in ([], {}, {"run_id": 1}, {"run_id": True, "user_id": 2},
                        {"run_id": -1, "user_id": 2}):
            with self.subTest(payload=payload):
                response = self.client.get("/download/" + self.signer.dumps(payload))
                self.assertEqual(response.status_code, 403)
        self.builder.assert_not_called()

    def test_missing_report_returns_404(self):
        self.builder.return_value = None
        response = self.client.get("/download/" + self.signer.dumps({"run_id": 12, "user_id": 3}))
        self.assertEqual(response.status_code, 404)
        self.pdf.assert_not_called()


if __name__ == "__main__":
    unittest.main()
