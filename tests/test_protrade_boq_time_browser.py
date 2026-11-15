"""Headless-browser checks for invisible BOQ activity/refresh handling."""
import json
from pathlib import Path
from urllib.parse import parse_qs
import unittest

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


@unittest.skipIf(sync_playwright is None, 'Playwright is not installed')
class BoqPreparationBrowserTests(unittest.TestCase):
    def setUp(self):
        self.runtime = sync_playwright().start()
        self.addCleanup(self.runtime.stop)
        self.browser = self.runtime.chromium.launch(headless=True)
        self.addCleanup(self.browser.close)
        self.page = self.browser.new_page()
        self.calls = []
        self.errors = []
        self.page.on('pageerror', lambda error: self.errors.append(str(error)))
        script = (Path(__file__).resolve().parents[1]/'app/static/js/boq_preparation.js').read_text()
        config = dict(url='/heartbeat', storage_key='boq-preparation:1:2:3', quote_key='', csrf='test-csrf')
        self.html = '<div id="quote-area-form"><form id="carpentry-quote-form"><input name="quote_key" type="hidden" value="' + 'a'*32 + '"><input id="work" aria-label="Work"></form></div>'
        self.html += '<script type="application/json" id="boq-preparation-config">'+json.dumps(config)+'</script><script>'+script+'</script>'
        def route(request):
            if request.request.url.endswith('/heartbeat'):
                if request.request.method == 'GET':
                    request.fulfill(status=200, content_type='application/json', body='{"csrf":"refreshed-csrf"}')
                    return
                self.calls.append({key:values[0] for key,values in parse_qs(request.request.post_data).items()})
                request.fulfill(status=204)
            else:
                request.fulfill(status=200, content_type='text/html', body=self.html)
        self.page.route('http://localhost:8937/**', route)
        self.page.clock.install()
        self.page.goto('http://localhost:8937/boq')
        self.page.wait_for_timeout(30)

    def flush(self):
        self.page.wait_for_timeout(30)
        self.assertEqual(self.errors, [])

    def test_automatic_activity_idle_resume_and_leave(self):
        self.assertTrue(self.calls)
        self.assertEqual(self.calls[-1]['action'], 'active')
        self.assertEqual(self.calls[-1]['csrf_token'], 'test-csrf')
        self.assertEqual(self.page.locator('button').count(), 0)
        self.assertNotIn('timer', self.page.locator('body').inner_text().lower())
        self.page.clock.run_for(60001)
        self.flush()
        self.assertEqual(self.calls[-1]['action'], 'pause')
        count = len(self.calls)
        self.page.clock.run_for(120000)
        self.flush()
        self.assertEqual(len(self.calls), count)
        self.page.locator('#work').fill('Preparing work')
        self.flush()
        self.assertEqual(self.calls[-1]['action'], 'active')
        self.page.evaluate("document.getElementById('quote-area-form').hidden = true")
        self.flush()
        self.assertEqual(self.calls[-1]['action'], 'pause')
        self.page.evaluate("document.getElementById('quote-area-form').hidden = false")
        self.flush()
        self.assertEqual(self.calls[-1]['action'], 'active')
        self.page.evaluate("Object.defineProperty(document,'hidden',{value:true,configurable:true});document.dispatchEvent(new Event('visibilitychange'))")
        self.flush()
        self.assertEqual(self.calls[-1]['action'], 'pause')

    def test_unsaved_key_survives_refresh_and_new_quote_gets_new_identity(self):
        key = self.page.locator('[name=quote_key]').input_value()
        self.page.reload()
        self.flush()
        self.assertEqual(self.page.locator('[name=quote_key]').input_value(), key)
        self.assertEqual(self.calls[-1]['quote_key'], key)
        self.page.evaluate("document.dispatchEvent(new Event('carpentry:new-quote'))")
        self.flush()
        new_key = self.page.locator('[name=quote_key]').input_value()
        self.assertNotEqual(new_key, key)
        self.assertEqual(self.calls[-1]['quote_key'], new_key)
        self.page.reload()
        self.flush()
        self.assertEqual(self.page.locator('[name=quote_key]').input_value(), new_key)

    def test_long_idle_session_refreshes_csrf_before_resuming(self):
        self.page.clock.run_for(3600001)
        self.flush()
        count = len(self.calls)
        self.page.locator('#work').fill('Continued work after a break')
        self.flush()
        self.assertGreater(len(self.calls), count)
        self.assertEqual(self.calls[-1]['action'], 'active')
        self.assertEqual(self.calls[-1]['csrf_token'], 'refreshed-csrf')


if __name__ == '__main__':
    unittest.main()
