"""Isolated METSOA PDF capacity and real Chromium regression checks; no DB."""
import ast
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / 'app/program_billing/metsoa_pdf_capacity.py'
spec = importlib.util.spec_from_file_location('pdf_capacity_test', MODULE)
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


class PdfCapacityTests(unittest.TestCase):
    def test_same_process_rejected_and_released(self):
        with capacity.browser_slot():
            with self.assertRaisesRegex(capacity.ServiceUnavailable, 'Another PDF'):
                with capacity.browser_slot():
                    self.fail('Concurrent browser admitted')
        with capacity.browser_slot():
            pass

    def test_other_process_rejected(self):
        script = """
import importlib.util, sys
s=importlib.util.spec_from_file_location('capacity',sys.argv[1])
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
try:
    with m.browser_slot(): pass
except m.ServiceUnavailable:
    sys.exit(23)
"""
        with capacity.browser_slot():
            result = subprocess.run([sys.executable, '-c', script, str(MODULE)], capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 23, result.stderr.decode())
        result = subprocess.run([sys.executable, '-c', script, str(MODULE)], capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_exception_releases_capacity(self):
        with self.assertRaisesRegex(RuntimeError, 'render failure'):
            with capacity.browser_slot():
                raise RuntimeError('render failure')
        with capacity.browser_slot():
            pass

    def test_actual_renderer_repeated_pdf_and_busy(self):
        # Execute the unchanged renderer body without importing Flask startup/DB.
        tree = ast.parse((ROOT / 'app/program_billing/municipal_metsoa.py').read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'pdf_bytes')
        function.body = [n for n in function.body if not (isinstance(n, ast.ImportFrom) and n.module == 'metsoa_pdf_capacity')]
        namespace = {'browser_slot': capacity.browser_slot}
        exec(compile(ast.Module(body=[function], type_ignores=[]), '<actual METSOA renderer>', 'exec'), namespace)
        render = namespace['pdf_bytes']
        with capacity.browser_slot():
            with self.assertRaises(capacity.ServiceUnavailable):
                render('<h1>Busy</h1>')
        with patch('playwright.sync_api.sync_playwright') as playwright:
            browser = playwright.return_value.__enter__.return_value.chromium.launch.return_value
            browser.new_page.return_value.set_content.side_effect = RuntimeError('render failed')
            with self.assertRaisesRegex(RuntimeError, 'render failed'):
                render('<h1>Failure</h1>')
            browser.close.assert_called_once_with()
        with patch('playwright.sync_api.sync_playwright') as playwright:
            playwright.return_value.__enter__.return_value.chromium.launch.side_effect = RuntimeError('launch failed')
            with self.assertRaisesRegex(RuntimeError, 'launch failed'):
                render('<h1>Failure</h1>')
        for _ in range(2):
            pdf = render('<h1>METSOA TEST</h1><table><tr><td>R56.61</td></tr></table>', test_mode=True)
            self.assertTrue(pdf.startswith(b'%PDF-'))
            self.assertGreater(len(pdf), 1000)


if __name__ == '__main__':
    unittest.main()
