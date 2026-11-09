"""Display-only signed differences, alignment, field borders and narrow tables."""
import copy
import importlib.util
import os
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
import unittest
from flask import Flask,render_template
from jinja2 import FileSystemLoader
from lxml import html
from pypdf import PdfReader
from test_pm_submission_layout import ROOT,sample,markup,namespace


def comparisons():
    return [dict(account=str(83192896040+i),calculated=Decimal(a),billed=Decimal(b),difference=Decimal(d),label=label) for i,(a,b,d,label) in enumerate([
        ('100','252.17','-152.17','Credit difference'),('200','165.76','34.24','Debit difference'),('300','1028.35','-728.35','Credit difference'),('50','50','0','No difference')])]


def reconciliation(rows=None):
    app=Flask('output-formatting');app.jinja_loader=FileSystemLoader(str(ROOT/'templates'))
    rows=comparisons() if rows is None else rows
    with app.app_context():
        body=render_template('program_billing/metsoa_comparison.html',pm_comparisons=rows,month='2026-10')
        document=render_template('program_billing/utilities_report_pdf.html',report='utilities_reconciliation',source='reconciliation',title='Utilities Reconciliation',item_label='All Accounts',property=dict(name='Layout QA',id=26,address=''),date_from='2026-10-01',date_to='2026-10-31',generated_at=datetime(2026,11,9),report_markup=body)
    return document,rows


class OutputFormattingTests(unittest.TestCase):
    def test_signed_difference_values_and_total_preserve_original_data(self):
        document,rows=reconciliation();before=copy.deepcopy(rows);tree=html.fromstring(document)
        self.assertEqual(tree.xpath('//*[@id="metsoa-comparison"]/tbody/tr/td[4]/text()'),['-R152.17','R34.24','-R728.35','R0.00'])
        self.assertEqual(tree.xpath('//*[@id="metsoa-comparison"]/tfoot/tr/td/strong/text()'),['R650.00','R1,496.28','-R846.28'])
        self.assertEqual(len(tree.xpath('//*[@id="metsoa-comparison"]/tbody/tr/td[@class="money"]')),12)
        self.assertFalse(tree.xpath('//*[@id="metsoa-comparison"]/tbody/tr/td[1]/@class'))
        self.assertEqual(rows,before)
        spec=importlib.util.spec_from_file_location('formatting_pdf_render',ROOT/'app/utils/pdf_render.py');renderer=importlib.util.module_from_spec(spec);spec.loader.exec_module(renderer)
        pdf=renderer.html_to_pdf_bytes(document);reader=PdfReader(BytesIO(pdf));self.assertEqual(len(reader.pages),1)
        text=reader.pages[0].extract_text()
        for value in ['-R152.17','R34.24','-R728.35','-R846.28']:self.assertIn(value,text)
        self.assertNotIn('Credit difference:',text);self.assertNotIn('Debit difference:',text)
        if os.environ.get('PM_OUTPUT_FORMATTING_QA'):
            path=Path(os.environ['PM_OUTPUT_FORMATTING_QA']);path.mkdir(parents=True,exist_ok=True)
            (path/'utilities-reconciliation.pdf').write_bytes(pdf)

    def test_estimated_disclosure_is_retained_outside_signed_amounts(self):
        rows=comparisons();rows[0]['estimated']=True;rows[0]['label']='Estimated Credit difference'
        document,_=reconciliation(rows);tree=html.fromstring(document)
        self.assertIn('Estimated comparisons.',tree.text_content())
        self.assertEqual(tree.xpath('//*[@id="metsoa-comparison"]/tbody/tr[1]/td[4]/text()'),['-R152.17'])

    def test_preview_and_print_monetary_alignment_and_centered_submission_tables(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(reconciliation()[0])
                for medium in ['screen','print']:
                    page.emulate_media(media=medium)
                    result=page.evaluate("""()=>{
                        const t=document.getElementById('metsoa-comparison');
                        return {amounts:[...t.querySelectorAll('.money')].map(c=>getComputedStyle(c).textAlign),account:getComputedStyle(t.querySelector('tbody td')).textAlign,
                        row:[...t.querySelectorAll('tbody tr:first-child td')].slice(1).map(c=>c.getBoundingClientRect().right),total:[...t.querySelectorAll('tfoot td')].map(c=>c.getBoundingClientRect().right)};
                    }""")
                    self.assertTrue(all(a=='right' for a in result['amounts']));self.assertEqual(result['account'],'left');self.assertEqual(result['row'],result['total'])
                page.set_content('<div style="width:178mm;margin:auto">'+markup(sample())+'</div>')
                boxes=page.evaluate("""()=>{const root=document.querySelector('.municipal-submission').getBoundingClientRect();return [...document.querySelectorAll('.municipal-submission table')].map(t=>{const r=t.getBoundingClientRect();return {width:r.width,offset:Math.abs((r.left+r.right)-(root.left+root.right))};});}""")
                for box in boxes:self.assertAlmostEqual(box['width'],152*96/25.4,delta=1);self.assertLess(box['offset'],1)
            finally:browser.close()
        if os.environ.get('PM_OUTPUT_FORMATTING_QA'):
            path=Path(os.environ['PM_OUTPUT_FORMATTING_QA']);path.mkdir(parents=True,exist_ok=True)
            (path/'municipal-submission.pdf').write_bytes(namespace['render_pdf'](sample()))

    def test_six_selection_fields_have_distinct_blue_borders_and_white_backgrounds(self):
        import test_pm_utilities_output as fixture
        from test_pm_directory_register import open_browser
        from playwright.sync_api import sync_playwright
        owner=fixture.UtilitiesOutputTests(methodName='test_email_pdf_failures_validation_and_stale_preview');owner.setUp();self.addCleanup(owner.doCleanups);self.addCleanup(owner.tearDown)
        response=owner.preview();self.assertEqual(response.status_code,200)
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                styles=page.evaluate("""()=>['output-source','output-report','output-item','output-month','output-from','output-to'].map(id=>{const s=getComputedStyle(document.getElementById(id));return [s.borderTopColor,s.borderTopWidth,s.backgroundColor,s.minHeight,s.padding];})""")
                self.assertEqual(styles,[['rgb(143, 180, 223)','1px','rgb(255, 255, 255)','42px','8px 12px']]*6)
            finally:browser.close()


if __name__=='__main__':unittest.main()
