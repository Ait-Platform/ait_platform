"""Deployment smoke-check failures must fail builds without app or DB startup."""
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('metsoa_browser_smoke',ROOT/'scripts/verify_metsoa_pdf_browser.py')
smoke=importlib.util.module_from_spec(spec);spec.loader.exec_module(smoke)


class PDFDeploymentTests(unittest.TestCase):
    def test_native_build_installs_matching_browser_and_checks_launch(self):
        script=(ROOT/'scripts/render_build.sh').read_text(encoding='utf-8')
        self.assertIn('set -euo pipefail',script)
        self.assertLess(script.index('export PLAYWRIGHT_BROWSERS_PATH=0'),script.index('python -m pip install -r requirements.txt'))
        self.assertLess(script.index('python -m pip install'),script.index('python -m playwright install chromium'))
        self.assertLess(script.index('python -m playwright install chromium'),script.index('python scripts/verify_metsoa_pdf_browser.py'))
        self.assertIn('python -m playwright install-deps chromium',script)
        self.assertIn('sudo -n true',script)
        self.assertNotIn('--no-shell',script)
        self.assertIn('playwright==1.60.0',(ROOT/'requirements.txt').read_text(encoding='utf-8-sig'))
        self.assertIn('PLAYWRIGHT_BROWSERS_PATH=0',(ROOT/'docs/render_metsoa_browser.md').read_text())

    def test_wrong_browser_path_fails_before_launch(self):
        with patch.dict(os.environ,PLAYWRIGHT_BROWSERS_PATH='/temporary/cache'),patch.object(smoke,'sync_playwright') as runtime:
            with self.assertRaisesRegex(RuntimeError,'must be 0'):smoke.verify()
            runtime.assert_not_called()

    def test_missing_browser_propagates_build_failure(self):
        runtime=Mock();runtime.chromium.launch.side_effect=RuntimeError('Executable missing')
        with patch.dict(os.environ,PLAYWRIGHT_BROWSERS_PATH='0'),patch.object(smoke,'sync_playwright') as launch:
            launch.return_value.__enter__.return_value=runtime
            with self.assertRaisesRegex(RuntimeError,'Executable missing'):smoke.verify()

    def test_pdf_failure_closes_browser_and_fails_build(self):
        runtime=Mock();browser=runtime.chromium.launch.return_value
        browser.new_page.return_value.pdf.return_value=b'not a pdf'
        with patch.dict(os.environ,PLAYWRIGHT_BROWSERS_PATH='0'),patch.object(smoke,'sync_playwright') as launch:
            launch.return_value.__enter__.return_value=runtime
            with self.assertRaisesRegex(RuntimeError,'valid PDF'):smoke.verify()
        browser.close.assert_called_once()

    def test_success_requires_headless_launch_and_valid_pdf(self):
        runtime=Mock();browser=runtime.chromium.launch.return_value
        browser.version='matching-browser';browser.new_page.return_value.pdf.return_value=b'%PDF-'+b'x'*1200
        with patch.dict(os.environ,PLAYWRIGHT_BROWSERS_PATH='0'),patch.object(smoke,'sync_playwright') as launch:
            launch.return_value.__enter__.return_value=runtime
            result=smoke.verify()
        runtime.chromium.launch.assert_called_once_with(headless=True)
        self.assertEqual(result['browser_path'],'0');self.assertGreater(result['pdf_bytes'],1000)
        browser.close.assert_called_once()


if __name__=='__main__':unittest.main()
