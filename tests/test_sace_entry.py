"""SACE commercial isolation checks without app startup or database writes."""
import ast
import unittest
from pathlib import Path
from unittest.mock import Mock
from flask import Flask, request, redirect

ROOT = Path(__file__).resolve().parents[1]


class EndorsementEntryTests(unittest.TestCase):
    def test_legacy_quote_links_redirect_before_commercial_work(self):
        tree = ast.parse((ROOT / "app/quote/routes.py").read_text(encoding="utf-8"))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "quote")
        fn.decorator_list = []
        env = dict(request=request, redirect=redirect, url_for=lambda endpoint: "/sace/about")
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<quote>", "exec"), env)
        app = Flask(__name__)
        for slug in ("sace", "sace_hub", "sace_endorsement"):
            with self.subTest(slug=slug), app.test_request_context("/quote?subject=" + slug):
                response = env["quote"]()
                self.assertEqual(response.location, "/sace/about")

    def test_endorsement_registration_ignores_stale_prices(self):
        tree = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8"))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "register")
        branch = next(n for n in fn.body if isinstance(n, ast.If)
                      and isinstance(n.test, ast.Compare)
                      and ast.unparse(n.test) == "subject in {'sace', 'sace_hub', 'sace_endorsement'}")
        lookup = Mock(side_effect=AssertionError("Endorsement must not price access"))
        for slug in ("sace", "sace_hub", "sace_endorsement"):
            env = dict(subject=slug, price_for_country=lookup, price_id=99,
                       local_cents=12345, est_zar_cents=12345, cc="US")
            exec(compile(ast.Module(body=[branch], type_ignores=[]), "<registration>", "exec"), env)
            self.assertEqual((env["cc"], env["cur"]), ("ZA", "ZAR"))
            self.assertEqual((env["local_cents"], env["est_zar_cents"]), (0, 0))
            self.assertIsNone(env["price_id"])
        lookup.assert_not_called()


    def test_dashboard_reentry_uses_shared_activity_resolver(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import patch
        tree = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8"))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "dashboard_info")
        fn.decorator_list = []
        response = redirect('/sace/provisioning')
        resolver = Mock(return_value=response)
        env = dict(current_user=SimpleNamespace(is_authenticated=True))
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<dashboard>", "exec"), env)
        with patch.dict(sys.modules, {'app.sace_activity': SimpleNamespace(
                explicit_activity=lambda **kwargs: object(), resolve_response=resolver)}):
            self.assertIs(env['dashboard_info']('sace_endorsement'), response)
            resolver.assert_called_once_with(subject='sace_endorsement')

    def test_login_resolver_precedes_generic_next_redirect(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import patch
        from urllib.parse import urlparse
        tree = ast.parse((ROOT / "app/auth/routes.py").read_text(encoding="utf-8"))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "login")
        start = next(i for i, n in enumerate(fn.body) if isinstance(n, ast.ImportFrom)
                     and n.module == "app.sace_activity")
        tail = ast.FunctionDef(name="after_authentication", args=ast.arguments(
            posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]),
            body=fn.body[start:], decorator_list=[])
        module = ast.fix_missing_locations(ast.Module(body=[tail], type_ignores=[]))
        env = dict(next_url='/sace/claim_code', redirect=redirect, request=request,
                   url_for=lambda endpoint: '/' + endpoint, _is_safe_url=lambda value: True,
                   urlparse=urlparse)
        exec(compile(module, '<login>', 'exec'), env)
        resolver = Mock(return_value=redirect('/sace/provisioning'))
        with Flask(__name__).test_request_context('/login'), patch.dict(sys.modules,
                {'app.sace_activity': SimpleNamespace(resolve_response=resolver)}):
            self.assertEqual(env['after_authentication']().location, '/sace/provisioning')
            resolver.return_value = None
            env['next_url'] = '/ordinary'
            self.assertEqual(env['after_authentication']().location, '/ordinary')
            env['next_url'] = None
            self.assertEqual(env['after_authentication']().location, '/auth_bp.bridge_dashboard')


if __name__ == "__main__":
    unittest.main()
