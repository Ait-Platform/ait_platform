"""Submission repair checks without application startup, database access or SMTP."""
import ast
import base64
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from flask import Flask, current_app, render_template
from flask_mail import Mail
import fitz

ROOT = Path(__file__).resolve().parents[1]
ASSETS = {'ait_logo.png': ('35d3ba88679f20d882f7c355d64b775530a8fc19', 282603),
          'ait_seal.png': ('aa52ddbd9e10b01cb2175a1d873a3e475e30ad38', 46897)}


def load_source(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SubmissionRepair(unittest.TestCase):
    def test_assets_are_exact_historical_git_blobs(self):
        for name, (expected, size) in ASSETS.items():
            data = (ROOT / 'static/branding' / name).read_bytes()
            self.assertEqual(len(data), size)
            self.assertEqual(hashlib.sha1(b'blob ' + str(size).encode() + b'\0' + data).hexdigest(), expected)

    def test_actual_workshop_generator_embeds_both_original_images(self):
        branding = load_source('repair_branding', 'app/utils/branding.py')
        renderer = load_source('repair_renderer', 'app/utils/pdf_render.py')
        tree = ast.parse((ROOT / 'app/program_sace/routes.py').read_text(encoding='utf-8-sig'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_generate_sace_certificate_pdf')
        namespace = {}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(ROOT / 'app/program_sace/routes.py'), 'exec'), namespace)
        app = Flask('repair', root_path=str(ROOT / 'app'), template_folder=str(ROOT / 'templates'))
        with patch.dict(sys.modules, {'app.utils.branding': branding, 'app.utils.pdf_render': renderer}), app.test_request_context('/certificate'), patch('smtplib.SMTP', side_effect=AssertionError('SMTP forbidden')), patch('smtplib.SMTP_SSL', side_effect=AssertionError('SMTP forbidden')):
            for name, helper in [('ait_logo.png', branding.get_logo_data_uri), ('ait_seal.png', branding.get_seal_data_uri)]:
                self.assertEqual(base64.b64decode(helper().split(',', 1)[1]), (ROOT / 'static/branding' / name).read_bytes())
            html = render_template('program_sace/post_test/certificate_pdf.html', learner_name='Local Auditor', completed_date='9 October 2026', certificate_id='AIT-WS-LOCAL', answers={'score': 100}, logo_path=branding.get_logo_data_uri(), seal_path=branding.get_seal_data_uri())
            for marker in ('Archoney Institute of Technology', 'Reading Workshop Certificate', 'auth-signature', 'AIT Official Seal', 'border: 4px solid #0033a1'):
                self.assertIn(marker, html)
            pdf = namespace['_generate_sace_certificate_pdf']('AIT-WS-LOCAL', 'Local Auditor', '2026-10-09T00:00:00', answers={'score': 100})
        self.assertTrue(pdf.startswith(b'%PDF-'))
        with fitz.open(stream=pdf, filetype='pdf') as document:
            self.assertIn('READING WORKSHOP CERTIFICATE', ''.join(p.get_text() for p in document))
            self.assertGreaterEqual(len({i[0] for p in document for i in p.get_images(full=True)}), 2)

    def test_real_configuration_and_factory_mail_initialization_capture_dummy_password(self):
        config_tree = ast.parse((ROOT / 'config.py').read_text(encoding='utf-8-sig'))
        config = next(n for n in config_tree.body if isinstance(n, ast.ClassDef) and n.name == 'Config')
        assignments = [n for n in config.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {'SMTP_PASSWORD', 'MAIL_PASSWORD'} for t in n.targets)]
        selected = ast.ClassDef(name='Config', bases=[], keywords=[], body=assignments, decorator_list=[])
        module = ModuleType('config')
        factory = next(n for n in ast.parse((ROOT / 'app/__init__.py').read_text(encoding='utf-8-sig')).body if isinstance(n, ast.FunctionDef) and n.name == 'create_app')
        first = next(i for i, n in enumerate(factory.body) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'app.config.from_object')
        last = next(i for i, n in enumerate(factory.body) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'mail.init_app')
        with tempfile.TemporaryDirectory() as instance, patch.dict(os.environ, {'SMTP_PASSWORD': 'dummy-local-secret'}, clear=True), patch('smtplib.SMTP', side_effect=AssertionError('SMTP forbidden')) as smtp, patch('smtplib.SMTP_SSL', side_effect=AssertionError('SMTP forbidden')) as ssl:
            exec(compile(ast.fix_missing_locations(ast.Module(body=[selected], type_ignores=[])), 'config.py', 'exec'), {'os': os}, module.__dict__)
            app = Flask('mail_repair', instance_path=instance, instance_relative_config=True)
            mail = Mail()
            with patch.dict(sys.modules, {'config': module}):
                exec(compile(ast.Module(body=factory.body[first:last + 1], type_ignores=[]), 'app/__init__.py', 'exec'), {'app': app, 'test_config': None, 'operator_mode': False, 'db': Mock(), 'csrf': Mock(), 'mail': mail})
            self.assertEqual(app.config['SMTP_PASSWORD'], 'dummy-local-secret')
            self.assertEqual(app.config['MAIL_PASSWORD'], 'dummy-local-secret')
            self.assertEqual(app.extensions['mail'].password, 'dummy-local-secret')
            smtp.assert_not_called()
            ssl.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
