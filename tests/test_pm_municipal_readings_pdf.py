"""Real portfolio readings and optional references on isolated local PostgreSQL."""
import importlib.util,unittest,os
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text,event
from alembic.operations import Operations
from alembic.migration import MigrationContext
from pypdf import PdfReader
from lxml import html
import test_pm_utilities_capture as fixture


class MunicipalReadingsPDFTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        before=self.snapshot()
        spec=importlib.util.spec_from_file_location('meter_reference_migration',Path('migrations/versions/pm_meter_reference_001.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.session.commit();self.conn.commit()
        after=self.snapshot()
        for name,rows in before.items():
            if name=='bil_meter':
                self.assertEqual(rows,[{k:v for k,v in r.items() if k!='municipal_reference'} for r in after[name]])
            else:self.assertEqual(rows,after[name])
        self.assertIsNone(self.session.execute(text('SELECT municipal_reference FROM bil_meter WHERE id=1')).scalar())
        from app.program_billing import municipal_readings_pdf
        self.pdf=municipal_readings_pdf
        for name,value in [('db',self.db),('current_user',self.user)]:
            mock=patch.object(self.pdf,name,value);mock.start();self.addCleanup(mock.stop)
        self.session.execute(text("UPDATE bil_muni_account SET water_meter_id=1 WHERE id=1"))
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,elec_meter_id) VALUES(2,27,'ACCOUNT-27',2),(3,28,'PRIVATE-ACCOUNT',3)"))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number,municipal_reference) VALUES(2,27,'ELEC-27','Electrical','ACCOUNT-27','E-REFERENCE-27'),(3,28,'PRIVATE-METER','Water','PRIVATE-ACCOUNT','PRIVATE-REFERENCE')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(1,'2026-09-30',999),(1,'2026-10-31',111.25),(1,'2026-11-01',888),(2,'2026-10-01',222),(2,'2026-10-31',223),(3,'2026-10-31',777)"))
        self.session.commit();self.conn.commit()
        self.url=self.readings_url+'?format=municipal_pdf&billing_month=2026-10'

    def test_reference_editor_save_lookup_clear_and_history_preservation(self):
        data=self.account_form(1);data.pop('csrf_token');data.pop('billing_month')
        self.assertIn('water_municipal_reference',data)
        self.assertIn('electricity_municipal_reference',data)
        data.update(owner_name='Owner',water_municipal_reference='W-REFERENCE-26')
        before=self.snapshot()
        self.assertEqual(self.post(data).status_code,302)
        saved=self.session.execute(text('SELECT meter_number,municipal_reference FROM bil_meter WHERE id=1')).one()
        self.assertEqual(tuple(saved),('WATER-26','W-REFERENCE-26'))
        self.assertEqual(self.post(data).status_code,409)  # Reference change invalidates stale form.
        created=self.post(dict(account_number='REFERENCE-NEW',owner_name='Owner',water_number='REFERENCE-W',electricity_number='REFERENCE-E',water_municipal_reference='W-NEW',electricity_municipal_reference='E-NEW'))
        self.assertEqual(created.status_code,302)
        references=dict(self.session.execute(text("SELECT meter_number,municipal_reference FROM bil_meter WHERE meter_number IN ('REFERENCE-W','REFERENCE-E')")).all())
        self.assertEqual(references,{'REFERENCE-W':'W-NEW','REFERENCE-E':'E-NEW'})
        for table in ('bil_meter_reading','bil_consumption','bil_architecture_draft'):
            self.assertEqual(before[table],self.snapshot()[table])
        lookup=self.client.get('/billing/utilities/26/accounts/lookup?account_number=MET-26',headers=self.headers).json
        self.assertEqual(lookup['values']['water_municipal_reference'],'W-REFERENCE-26')
        data=self.account_form(1);data.pop('csrf_token');data.pop('billing_month')
        data.update(owner_name='Owner',water_municipal_reference='')
        self.assertEqual(self.post(data).status_code,302)
        self.assertIsNone(self.session.execute(text('SELECT municipal_reference FROM bil_meter WHERE id=1')).scalar())
        data=self.account_form(1);data.pop('csrf_token');data.pop('billing_month')
        data.update(owner_name='Owner',water_municipal_reference='x'*101)
        snapshot=self.snapshot();self.assertEqual(self.post(data).status_code,400);self.assertEqual(snapshot,self.snapshot())
        self.assertEqual(self.post(dict(data,account_id='3',water_municipal_reference='W-VALID')).status_code,404)

    def test_pdf_uses_all_authorized_recorded_period_rows_and_blank_references(self):
        before=self.snapshot();queries=[]
        def record(conn,cursor,sql,params,context,many):queries.append(sql.lower().lstrip())
        event.listen(self.conn,'before_cursor_execute',record)
        try:response=self.client.get(self.url,headers=self.headers)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200,response.text if response.mimetype!='application/pdf' else '')
        self.assertEqual(response.mimetype,'application/pdf')
        self.assertIn('attachment;',response.headers['Content-Disposition'])
        self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        reader=PdfReader(BytesIO(response.data));contents='\n'.join(p.extract_text() for p in reader.pages)
        for expected in ('Electricity Readings','Water Readings','MET-26','ACCOUNT-27','WATER-26','ELEC-27','E-REFERENCE-27','111.25','222','223','2026-10-31','MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT','Name:','Signature:','Date:','Municipal Stamp:'):
            self.assertIn(expected,contents)
        for absent in ('PRIVATE','999','888','777','2026-09-30','2026-11-01'):
            self.assertNotIn(absent,contents)
        with self.app.test_request_context():
            data=self.pdf.report(26,'2026-10')
        self.assertEqual(len(data['groups']['water']),1)
        self.assertIsNone(data['groups']['water'][0]['municipal_reference'])
        self.assertEqual(len(data['groups']['electricity']),2)
        for page in reader.pages:
            self.assertAlmostEqual(float(page.mediabox.width),595.2756,places=2)
            self.assertAlmostEqual(float(page.mediabox.height),841.8898,places=2)
        self.assertFalse(any(sql.startswith(('insert','update','delete','alter','create')) for sql in queries))
        self.assertEqual(before,self.snapshot())
        if os.environ.get('PM_READINGS_PDF_QA_DIR'):
            directory=Path(os.environ['PM_READINGS_PDF_QA_DIR']);directory.mkdir(parents=True,exist_ok=True)
            (directory/'municipal-readings-synthetic-qa.pdf').write_bytes(response.data)

    def test_workflow_link_empty_period_auth_and_association_checks(self):
        page=self.client.get(self.readings_url+'?billing_month=2026-10',headers=self.headers)
        tree=html.fromstring(page.data)
        selector=tree.xpath('//form[@id="municipal-reading-period"]')[0]
        self.assertEqual(selector.get('action'),self.readings_url)
        self.assertEqual(selector.xpath('.//input[@name="billing_month"]/@value'),['2026-10'])
        self.assertEqual(tree.xpath('//button[@form="municipal-reading-period"][@name="format"]/@value'),['municipal_pdf'])
        self.assertFalse(tree.xpath('//*[@id="municipal-reading-scope"]'))
        self.assertNotIn('Recorded in ',tree.xpath('//*[@id="reading-register"]')[0].text_content())
        self.assertFalse(tree.xpath('//button[text()="Show Period"]'))
        self.assertFalse(tree.xpath('//input[@type="month"]'))
        self.assertEqual(selector.xpath('.//input[@name="billing_month"]/@type'),['hidden'])
        self.assertTrue(tree.xpath('//select[@id="reading-meter-filter"]'))
        self.assertTrue(tree.xpath('//button[@id="reading-output"]'))
        self.assertTrue(tree.xpath('//tr[@data-reading-row]'))
        self.assertEqual(self.client.get(self.url).status_code,403)
        self.assertEqual(self.client.get(self.url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        for month in ('bad','2026-13'):
            self.assertEqual(self.client.get(self.url.replace('2026-10',month),headers=self.headers).status_code,400)
        self.assertEqual(self.client.get(self.url+'&billing_month=2026-11',headers=self.headers).status_code,400)
        empty=self.client.get(self.url.replace('2026-10','2026-08'),headers=self.headers)
        contents=' '.join(p.extract_text() for p in PdfReader(BytesIO(empty.data)).pages)
        self.assertEqual(contents.count('No recorded readings for this period.'),2)
        self.assertNotIn('111.25',contents)
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,water_meter_id) VALUES(4,26,'AMBIGUOUS',1)"));self.session.commit();self.conn.commit()
        before=self.snapshot()
        self.assertEqual(self.client.get(self.url,headers=self.headers).status_code,409)
        self.assertEqual(before,self.snapshot())

    def test_pdf_without_reference_column_exports_recorded_readings_with_blank_reference(self):
        import types
        before=self.snapshot()
        with patch.object(self.pdf,'inspect',return_value=types.SimpleNamespace(get_columns=lambda table:[])):
            response=self.client.get(self.url,headers=self.headers)
        self.assertEqual(response.status_code,200)
        contents=' '.join(p.extract_text() for p in PdfReader(BytesIO(response.data)).pages)
        self.assertIn('111.25',contents);self.assertIn('223.0',contents)
        self.assertNotIn('E-REFERENCE-27',contents)
        self.assertEqual(before,self.snapshot())

    def test_full_seven_electricity_six_water_portfolio_is_not_limited_to_current_property(self):
        # Existing fixture has 2 electricity rows and 1 water row in October.
        for i in range(10):
            meter=100+i;property_id=26 if i%2 else 27
            utility='Electrical' if i<5 else 'Water';account='PORTFOLIO-'+str(i)
            self.session.execute(text('INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(:id,:p,:account)'),dict(id=meter,p=property_id,account=account))
            self.session.execute(text('INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(:id,:p,:number,:utility,:account)'),dict(id=meter,p=property_id,number='PERIOD-METER-'+str(i),utility=utility,account=account))
            self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:id,'2026-10-30',:value)"),dict(id=meter,value=1000+i))
        self.session.commit();self.conn.commit()
        before=self.snapshot()
        with self.app.test_request_context():data=self.pdf.report(26,'2026-10')
        self.assertEqual(len(data['groups']['electricity']),7)
        self.assertEqual(len(data['groups']['water']),6)
        page=self.client.get(self.readings_url+'?billing_month=2026-10',headers=self.headers)
        self.assertFalse(html.fromstring(page.data).xpath('//*[@id="municipal-reading-scope"]'))
        response=self.client.get(self.url,headers=self.headers)
        contents=' '.join(p.extract_text() for p in PdfReader(BytesIO(response.data)).pages)
        for i in range(10):self.assertIn('PERIOD-METER-'+str(i),contents)
        with self.app.test_request_context():september=self.pdf.report(26,'2026-09')
        self.assertEqual(len(september['groups']['electricity']),0)
        self.assertEqual(len(september['groups']['water']),1)
        self.assertEqual(before,self.snapshot())

    def test_multipage_pdf_keeps_all_rows_and_receipt_together(self):
        with self.app.test_request_context():data=self.pdf.report(26,'2026-10')
        row=data['groups']['electricity'][0]
        data['groups']['electricity']=[dict(row,meter_number='LONG-METER-'+str(i),municipal_reference='E-REFERENCE-'+str(i)+'-WITH-LONG-TEXT',reading=str(1000+i)) for i in range(65)]
        pdf=self.pdf.render_pdf(data);reader=PdfReader(BytesIO(pdf))
        self.assertGreater(len(reader.pages),1)
        self.assertIn('LONG-METER-0',reader.pages[0].extract_text())
        contents=' '.join(page.extract_text() for page in reader.pages)
        for i in range(65):self.assertIn('LONG-METER-'+str(i),contents)
        last=reader.pages[-1].extract_text()
        for label in ('MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT','Name:','Signature:','Date:','Municipal Stamp:'):self.assertIn(label,last)
        if os.environ.get('PM_READINGS_PDF_QA_DIR'):
            (Path(os.environ['PM_READINGS_PDF_QA_DIR'])/'municipal-readings-multipage-synthetic-qa.pdf').write_bytes(pdf)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(MunicipalReadingsPDFTests(name) for name in MunicipalReadingsPDFTests.__dict__ if name.startswith('test_'))
