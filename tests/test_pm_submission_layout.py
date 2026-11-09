"""Compact municipal submission only; representative 7+6 meter layout fixtures."""
import ast
import copy
import os
from datetime import date
from io import BytesIO
from pathlib import Path
from xml.sax.saxutils import escape
import unittest
from flask import Flask, render_template
from jinja2 import FileSystemLoader
from pypdf import PdfReader

ROOT=Path(__file__).resolve().parents[1]
namespace=dict(BytesIO=BytesIO,escape=escape)
tree=ast.parse((ROOT/'app/program_billing/municipal_readings_pdf.py').read_text(encoding='utf-8'))
functions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in {'presentation','render_pdf'}]
exec(compile(ast.Module(body=functions,type_ignores=[]),'<production-municipal-layout>','exec'),namespace)


def sample():
    groups={'electricity':[],'water':[]}
    for utility,count in [('electricity',7),('water',6)]:
        for number in range(count):
            groups[utility].append(dict(account_number=str(83192896040+number),municipal_reference=('E' if utility=='electricity' else 'W')+str(4049314+number),meter_number=('QA-E-METER-' if utility=='electricity' else 'QA-W-METER-')+str(number+1).zfill(2),reading=str(95000+number if utility=='electricity' else 15000+number),reading_date=date(2026,10,30)))
    return dict(month='2026-10',groups=groups)


def markup(data):
    app=Flask('municipal-layout');app.jinja_loader=FileSystemLoader(str(ROOT/'templates'))
    with app.app_context():
        body=render_template('program_billing/output_municipal_submission.html',**data,**namespace['presentation'](data))
        return render_template('program_billing/utilities_report_content.html',report='municipal_submission',report_markup=body)


class MunicipalSubmissionLayoutTests(unittest.TestCase):
    def test_thirteen_rows_one_a4_page_common_date_once_and_receipt(self):
        data=sample();before=copy.deepcopy(data)
        pdf=namespace['render_pdf'](data);reader=PdfReader(BytesIO(pdf))
        self.assertEqual(len(reader.pages),1)
        page=reader.pages[0];self.assertAlmostEqual(float(page.mediabox.width),595.28,places=1);self.assertAlmostEqual(float(page.mediabox.height),841.89,places=1)
        text=page.extract_text()
        self.assertIn('Reporting Month: October 2026',text)
        self.assertEqual(text.count('Reading Date: 30 October 2026'),1)
        self.assertNotIn('2026-10-30',text)
        for label in ['MUNICIPAL METER READINGS','Electricity Readings (kWh)','Water Readings (kL)','MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT','Name:','Signature:','Date:','Municipal Stamp:']:self.assertIn(label,text)
        for row in sum(data['groups'].values(),[]):
            for key in ['account_number','municipal_reference','meter_number','reading']:self.assertIn(row[key],text)
        self.assertEqual(data,before)
        from lxml import html
        preview=html.fromstring(markup(data));self.assertEqual(len(preview.xpath('//tbody/tr')),13)
        self.assertEqual(len(preview.xpath('//span[@class="capture-checkbox"]')),13)
        self.assertFalse(preview.xpath('//span[@class="reading-date"]'))
        self.assertEqual(preview.xpath('//h2/text()'),['MUNICIPAL METER READINGS'])
        if os.environ.get('PM_SUBMISSION_LAYOUT_QA'):
            path=Path(os.environ['PM_SUBMISSION_LAYOUT_QA']);path.mkdir(parents=True,exist_ok=True)
            (path/'municipal-submission-13-meter-qa.pdf').write_bytes(pdf)
            (path/'municipal-submission-13-meter-qa.html').write_text(markup(data),encoding='utf-8')

    def test_differing_dates_are_preserved_individually(self):
        data=sample();data['groups']['water'][0]['reading_date']=date(2026,10,29)
        self.assertIsNone(namespace['presentation'](data)['common_date'])
        reader=PdfReader(BytesIO(namespace['render_pdf'](data)));self.assertEqual(len(reader.pages),1)
        text=reader.pages[0].extract_text();self.assertIn('Reading dates vary',text)
        self.assertIn('2026-10-29',text);self.assertEqual(text.count('2026-10-30'),12)
        from lxml import html
        preview=html.fromstring(markup(data));dates=preview.xpath('//span[@class="reading-date"]/text()')
        self.assertEqual(len(dates),13);self.assertIn('2026-10-29',dates)
        self.assertNotIn('Reading Date:',preview.text_content())

    def test_print_preview_fits_one_a4_page(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(markup(sample()));page.emulate_media(media='print')
                pdf=page.pdf(format='A4',print_background=True,prefer_css_page_size=True)
                reader=PdfReader(BytesIO(pdf));self.assertEqual(len(reader.pages),1)
                self.assertIn('MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT',reader.pages[0].extract_text())
                checkbox=page.locator('.capture-checkbox').first.bounding_box();self.assertGreaterEqual(checkbox['width'],18)
                stamp=page.locator('.municipal-stamp').bounding_box();self.assertGreaterEqual(stamp['height'],130)
                if os.environ.get('PM_SUBMISSION_LAYOUT_QA'):
                    path=Path(os.environ['PM_SUBMISSION_LAYOUT_QA']);path.mkdir(parents=True,exist_ok=True)
                    (path/'municipal-submission-print-qa.pdf').write_bytes(pdf)
            finally:browser.close()

    def test_empty_and_multi_month_labels_do_not_invent_common_dates(self):
        data=dict(month='2026-09-01 to 2026-10-31',groups=dict(electricity=[],water=[]))
        display=namespace['presentation'](data)
        self.assertEqual(display['month_label'],data['month']);self.assertIsNone(display['reading_date_label'])
        self.assertEqual(len(PdfReader(BytesIO(namespace['render_pdf'](data))).pages),1)


if __name__=='__main__':unittest.main()
