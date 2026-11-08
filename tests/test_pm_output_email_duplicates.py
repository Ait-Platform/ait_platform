"""Utilities-only durable email receipts, failures and double-submit UI."""
import unittest
from unittest.mock import patch
from pathlib import Path
from sqlalchemy import text
import test_pm_utilities_output as fixture


class OutputEmailDuplicateTests(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.UtilitiesOutputTests(methodName='test_email_pdf_failures_validation_and_stale_preview')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.addCleanup(self.fx.tearDown)
        self.output = self.fx.output
        self.fields = self.fx.email_fields(self.fx.preview())

    def post(self, **changes):
        return self.fx.client.post(self.fx.base, data=dict(self.fields, **changes), headers=self.fx.headers)

    def test_repeated_and_inflight_request_send_once_and_record_acceptance(self):
        outcomes = []
        def send(*args):
            outcomes.append(self.post().status_code)
            return True
        with patch.object(self.output, 'render_pdf', return_value=b'%PDF-test'), patch.object(self.output, 'email_pdf', side_effect=send) as sender:
            self.assertEqual(self.post().status_code, 302)
            replay = self.post()
            self.assertEqual(replay.status_code, 409)
            self.assertIn('already been submitted', replay.text)
            sender.assert_called_once()
        self.assertEqual(outcomes, [409])
        states = self.fx.session.execute(text("SELECT after_data->>'status' FROM bil_utilities_capture_audit WHERE kind='output_email' ORDER BY id")).scalars().all()
        self.assertEqual(states, ['attempted', 'accepted'])

    def test_uncertain_failure_is_not_retried_or_reported_as_success(self):
        with patch.object(self.output, 'render_pdf', return_value=b'%PDF-test'), patch.object(self.output, 'email_pdf', side_effect=RuntimeError('SMTP failed')) as sender:
            with self.assertLogs(self.fx.app.logger, level='ERROR'):
                result = self.post()
            self.assertEqual(result.status_code, 503)
            self.assertNotIn('Mail transport accepted', result.text)
            returned = self.fx.email_fields(result)
            self.assertEqual(returned['preview_token'], self.fields['preview_token'])
            self.assertEqual(self.fx.client.post(self.fx.base, data=returned, headers=self.fx.headers).status_code, 409)
            sender.assert_called_once()
        self.assertEqual(self.fx.session.execute(text("SELECT count(*) FROM bil_utilities_capture_audit WHERE kind='output_email' AND after_data->>'status'='accepted'")).scalar(), 0)

    def test_receipt_failure_prevents_smtp(self):
        self.fx.session.execute(text("ALTER TABLE bil_utilities_capture_audit ADD CONSTRAINT reject_email CHECK(kind<>'output_email')"))
        self.fx.session.commit(); self.fx.conn.commit()
        with patch.object(self.output, 'render_pdf', return_value=b'%PDF-test'), patch.object(self.output, 'email_pdf') as sender:
            with self.assertLogs(self.fx.app.logger, level='ERROR'):
                self.assertEqual(self.post().status_code, 503)
            sender.assert_not_called()

    def test_distinct_recipient_is_a_distinct_request(self):
        with patch.object(self.output, 'render_pdf', return_value=b'%PDF-test'), patch.object(self.output, 'email_pdf', return_value=True) as sender:
            self.assertEqual(self.post().status_code, 302)
            self.assertEqual(self.post(email_to='another@example.com').status_code, 302)
            self.assertEqual(sender.call_count, 2)

    def test_postgres_claim_lock_excludes_another_connection(self):
        # Exercise the same transaction lock used by independent web workers.
        key = 'pm-utilities-email:isolated-regression-lock'
        with self.fx.conn.engine.connect() as other:
            self.fx.session.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key,0))'), dict(key=key))
            self.assertFalse(other.execute(text('SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))'), dict(key=key)).scalar())
            other.rollback()
            self.fx.session.commit(); self.fx.conn.commit()
            self.assertTrue(other.execute(text('SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))'), dict(key=key)).scalar())
            other.rollback()

    def test_false_transport_result_never_reports_acceptance(self):
        with patch.object(self.output, 'render_pdf', return_value=b'%PDF-test'), patch.object(self.output, 'email_pdf', return_value=False):
            with self.assertLogs(self.fx.app.logger, level='ERROR'):
                result = self.post()
            self.assertEqual(result.status_code, 503)
            self.assertNotIn('Mail transport accepted', result.text)

    def test_browser_blocks_second_submit_and_disables_email_controls(self):
        from playwright.sync_api import sync_playwright
        page = self.fx.preview()
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(headless=True)
            try:
                tab = browser.new_page()
                tab.route('**/*', lambda route: route.abort())
                tab.set_content('<base href="https://ait.example/">' + page.text)
                tab.add_script_tag(path='app/static/js/pm_utilities_output.js')
                tab.locator('#output-email').click()
                results = tab.evaluate("""() => {
                    const f=document.getElementById('output-email-form');
                    const first=f.dispatchEvent(new Event('submit',{cancelable:true}));
                    const second=f.dispatchEvent(new Event('submit',{cancelable:true}));
                    return [first,second,new FormData(f).get('action')];
                }""")
                self.assertEqual(results, [True, False, 'email'])
                self.assertTrue(tab.locator('#output-email').is_disabled())
                self.assertTrue(tab.locator('#output-email-form button').is_disabled())
            finally:
                browser.close()


if __name__ == '__main__':
    unittest.main()
