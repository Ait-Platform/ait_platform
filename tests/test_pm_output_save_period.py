"""Reporting-period filenames and METSOA-only cooldown, with no real email/DB writes."""
from datetime import date,datetime
import unittest
from playwright.sync_api import sync_playwright
import test_pm_central_utilities_output as fixture
from test_pm_directory_register import open_browser


class OutputSavePeriodTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records');self.owner.setUp();self.addCleanup(self.owner.doCleanups)
        self.fx=self.owner.fx;self.output=self.owner.output

    def page(self,report='metsoa_statement'):
        return self.owner.get('metsoa' if report=='metsoa_statement' else 'readings',report,recipient_id='11',by_month='2026-10')

    def test_period_end_precedence_custom_ranges_history_and_snapshot(self):
        end=self.output.report_end_date
        base=dict(report='history',date_to='2026-10-31',rows=[dict(reading_date=date(2026,10,30))],generated_at=datetime(2026,11,9))
        self.assertEqual(end(base),'2026-10-31')
        self.assertEqual(end(dict(base,date_to='2026-10-15')),'2026-10-15')
        self.assertEqual(end(dict(base,date_to='')),'2026-10-30')
        self.assertEqual(end(dict(base,report='arrangement_register',date_to='',billing_month='2026-10')),'2026-10-31')
        self.assertEqual(end(dict(base,report='account_register',date_to='',rows=[])),'2026-11-09')

    def test_actual_metsoa_context_names_duplicates_from_period_not_pc_date(self):
        response=self.page();self.assertEqual(response.status_code,200,response.text)
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                result=page.evaluate("""async()=>{
                    const scope=JSON.parse(document.getElementById('output-directory-context').textContent),root=await navigator.storage.getDirectory();
                    await AITDirectories.set(scope,root);let count=0;
                    window.fetch=async()=>new Response('%PDF-'+(++count));
                    const names=[];for(let n=0;n<3;n++)names.push(await AITDirectories.save(scope,scope.report,'/pdf'));
                    const folder=await (await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement'),contents=[];
                    for(const name of names)contents.push(await (await (await folder.getFileHandle(name)).getFile()).text());
                    return {end:scope.period_end,names,contents};
                }""")
                self.assertEqual(result['end'],'2026-10-31')
                self.assertEqual(result['names'],['311026_METSOA.pdf','311026_METSOA(1).pdf','311026_METSOA(2).pdf'])
                self.assertEqual(result['contents'],['%PDF-1','%PDF-2','%PDF-3'])
            finally:browser.close()

    def test_metsoa_native_print_title_no_fetch_and_ten_second_cooldown(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=self.page().text)
            try:
                page.clock.install();old_title=page.title()
                page.evaluate("""()=>{window.printCalls=[];window.fetch=()=>{throw new Error('Unexpected PDF request');};AITDirectories.save=()=>{throw new Error('Unexpected folder save');};window.print=()=>printCalls.push(document.title);}""")
                page.locator('#output-save').click()
                page.evaluate("document.getElementById('output-save').dispatchEvent(new MouseEvent('click'))")
                self.assertEqual(page.evaluate('printCalls'),['311026_METSOA'])
                self.assertTrue(page.locator('#output-save').is_disabled())
                self.assertTrue(page.locator('html').evaluate("e=>e.hasAttribute('data-metsoa-native-save')"))
                self.assertIn('Saving or cancellation is controlled by your browser',page.locator('#output-save-message').inner_text())
                page.evaluate("window.dispatchEvent(new Event('afterprint'))")
                self.assertEqual(page.title(),old_title)
                self.assertFalse(page.locator('html').evaluate("e=>e.hasAttribute('data-metsoa-native-save')"))
                self.assertEqual(page.locator('#output-save').inner_text(),'Save Report (10s)')
                page.clock.fast_forward(10000);self.assertTrue(page.locator('#output-save').is_enabled())
                page.locator('#output-month').fill('2026-07');page.locator('#output-month').dispatch_event('change')
                self.assertTrue(page.locator('#output-save').is_disabled())
            finally:browser.close()

    def test_other_reports_have_no_cooldown_and_dirty_selection_stays_disabled(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=self.page('history').text)
            try:
                page.clock.install()
                page.evaluate("()=>{window.calls=0;AITDirectories.save=async()=>{calls++;return '311026_Reading_History.pdf';};}")
                for _ in range(2):
                    page.locator('#output-save').click();page.wait_for_function("!document.getElementById('output-save').disabled")
                self.assertEqual(page.evaluate('calls'),2)
                self.assertEqual(page.locator('#output-save').inner_text(),'Save Report')
                page.select_option('#output-source','reconciliation');page.clock.fast_forward(11000)
                self.assertTrue(page.locator('#output-save').is_disabled())
            finally:browser.close()

    def test_metsoa_native_print_failure_restores_title_and_download(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=self.page().text)
            try:
                old_title=page.title();url=page.locator('#output-download').get_attribute('href')
                page.evaluate("()=>{window.print=()=>{throw new Error('Print unavailable');};}")
                page.locator('#output-save').click()
                self.assertEqual(page.title(),old_title)
                self.assertFalse(page.locator('html').evaluate("e=>e.hasAttribute('data-metsoa-native-save')"))
                self.assertTrue(page.locator('#output-save').is_enabled())
                self.assertEqual(page.locator('#output-download').get_attribute('href'),url)
                self.assertIn('No PDF saving was confirmed',page.locator('#output-save-message').inner_text())
            finally:browser.close()


if __name__=='__main__':unittest.main()
