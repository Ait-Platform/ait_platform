"""Read-only generation and property/month isolation on temporary PostgreSQL."""
import unittest
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import text,event
from lxml import html
import test_pm_utilities_capture as fixture
class MunicipalMetsoaTests(fixture.UtilitiesCaptureTests):
    generation_url='/billing/utilities/26/generate-metsoa?billing_month=2026-10'
    def setUp(self):
        super().setUp()
        self.app.add_url_rule('/test-checkout/<month>',endpoint='billing_bp.billing_checkout',view_func=lambda **kw:'Existing checkout')
        from app.program_billing import municipal_metsoa
        for name,value in [('db',self.db)]:
            mock=patch.object(municipal_metsoa,name,value);mock.start();self.addCleanup(mock.stop)
        self.metsoa=municipal_metsoa;self.user.has_role=lambda role:False
        self.session.execute(text('CREATE TEMP TABLE bil_statement_payment(id integer PRIMARY KEY,manager_id integer,month text,amount_paid_cents integer)'))
        self.session.execute(text("INSERT INTO bil_statement_payment VALUES(1,7,'2026-10',10000)"))
        self.session.execute(text('CREATE TEMP TABLE bil_muni_payment(id integer PRIMARY KEY,account_id integer,month text,amount numeric(18,2))'))
        self.session.execute(text("INSERT INTO bil_muni_payment VALUES(1,1,'2026-10',100.25),(2,1,'2026-09',999),(3,99,'2026-10',9999)"))
        self.session.execute(text("UPDATE bil_property SET name='Almond' WHERE id=26"))
        self.session.execute(text("UPDATE bil_muni_account SET account_number='83192896040',water_meter_id=1,elec_meter_id=4 WHERE id=1"))
        self.session.execute(text("UPDATE bil_meter SET meter_number='CEL888',municipal_bill_number='83192896040' WHERE id=1"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-09-23',reading_value=0 WHERE id=1"))
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,water_meter_id,elec_meter_id) VALUES(2,26,'83347721969',2,3)"))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(2,26,'CXN998','Water','83347721969'),(3,26,'7150696S','Electrical','83347721969'),(4,26,'2188802S','Electrical','83192896040'),(5,26,'ANG489','Water','83192896040')"))
        for meter,day,value in [(1,'2026-10-23',31),(1,'2026-11-23',9999),(2,'2026-09-11',614),(2,'2026-10-11',615),(3,'2026-08-26',49577),(3,'2026-10-26',49592),(4,'2026-08-26',30025),(4,'2026-10-26',30572),(5,'2026-10-25',20)]:
            self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(1,'2026-10','2026-10-28',7043.55,7190.88,7190.88,'2026-11-07')"));self.session.commit();self.conn.commit()
    def test_read_only_generation_uses_actual_pairs_bill_and_payment_sources(self):
        before=self.snapshot();writes=[]
        def record(conn,cursor,sql,params,context,many):
            if sql.lstrip().lower().startswith(('insert ','update ','delete ')):writes.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            with self.app.test_request_context():data=self.metsoa.build(26,'2026-10')
            page=self.client.get(self.generation_url,headers=self.headers);self.assertEqual(page.status_code,200,page.text)
            self.assertEqual(self.client.get(self.generation_url,headers=self.headers).status_code,200)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        rows={r['number']:r for a in data['accounts'] for r in a['readings']}
        for number,value in [('CEL888',31),('CXN998',1),('7150696S',15),('2188802S',547)]:self.assertEqual(rows[number]['difference'],Decimal(value))
        self.assertEqual(rows['7150696S']['previous']['date'].isoformat(),'2026-08-26');self.assertEqual(rows['CEL888']['current']['date'].isoformat(),'2026-10-23')
        self.assertIsNone(rows['ANG489']['difference']);self.assertIn('Only one reading',rows['ANG489']['error'])
        self.assertEqual(data['accounts'][0]['paid'],Decimal('100.25'));self.assertEqual(data['accounts'][0]['bill']['due'],Decimal('7190.88'))
        self.assertFalse(data['complete']);self.assertFalse(data['bill_complete']);self.assertIn('83347721969',page.text);self.assertIn('Incomplete statement',page.text)
        self.assertIn('Open Metro Reconciliation',page.text);self.assertIn('billing_month=2026-10',page.text)
        self.assertFalse(writes);self.assertEqual(before,self.snapshot())
    def test_consolidated_totals_only_when_all_account_bills_exist(self):
        self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(2,'2026-10','2026-10-28',1.01,2.02,3.03,'2026-11-07')"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10')
        self.assertTrue(data['bill_complete']);self.assertEqual(data['totals']['due'],Decimal('7193.91'));self.assertEqual(data['totals']['paid'],Decimal('100.25'))
        self.assertFalse(data['complete']) # ANG489 still has one reading
    def test_complete_statement_when_all_genuine_readings_and_bills_are_present(self):
        self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(2,'2026-10','2026-10-28',0,10.00,10.00,'2026-11-07')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-25',10)"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10')
        self.assertTrue(data['complete']);self.assertTrue(data['bill_complete'])
        page=self.client.get(self.generation_url,headers=self.headers);self.assertEqual(page.status_code,200)
        self.assertNotIn('Incomplete statement',page.text);self.assertIn('R7,200.88',page.text)

    def test_permissions_month_validation_and_token_free_pm_access(self):
        self.assertEqual(self.client.get(self.generation_url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.generation_url).status_code,403)
        self.assertEqual(self.client.get(self.generation_url.replace('2026-10','2026-19'),headers=self.headers).status_code,400)
        unpurchased=self.client.get(self.generation_url.replace('2026-10','2026-09'),headers=self.headers);self.assertEqual(unpurchased.status_code,200);self.assertIn('Incomplete statement',unpurchased.text)
    def test_october_manager_with_zero_tokens_needs_no_unlock_or_financial_writes(self):
        self.session.execute(text('DELETE FROM bil_statement_payment'))
        self.session.execute(text('CREATE TEMP TABLE ait_token_wallet(id integer PRIMARY KEY,user_id integer,balance integer)'))
        self.session.execute(text('INSERT INTO ait_token_wallet VALUES(1,7,0)'));self.session.commit();self.conn.commit()
        before=self.snapshot();queries=[]
        def record(conn,cursor,sql,params,context,many):queries.append(sql.lower())
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            for _ in range(2):
                response=self.client.get(self.generation_url,headers=self.headers)
                self.assertEqual(response.status_code,200,response.text);self.assertIn('Municipal Charges Statement',response.text)
                self.assertNotIn('unlock statements',response.text);self.assertNotIn('Insufficient tokens',response.text)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertFalse(any('bil_statement_payment' in q or 'token_wallet' in q or 'token_transaction' in q for q in queries))
        self.assertFalse(any(q.lstrip().startswith(('insert ','update ','delete ')) for q in queries))
        self.assertEqual(before,self.snapshot());self.assertEqual(self.session.execute(text('SELECT balance FROM ait_token_wallet WHERE id=1')).scalar(),0)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_statement_payment')).scalar(),0)

    def test_reading_five_row_display_and_navigation_preserve_property_month(self):
        page=self.client.get(self.readings_url+'?billing_month=2026-10',headers=self.headers);self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);rows=tree.xpath('//tr[@data-reading-row]')
        self.assertEqual(len(rows),10);self.assertEqual(len(tree.xpath('//tr[@data-reading-row][not(@hidden)]')),5)
        self.assertEqual(rows[0].xpath('./td')[0].text_content(),'2026-11-23')
        self.assertEqual(len(tree.xpath('//tr[@data-reading-row]//a[contains(@href,"edit=")]')),10)
        self.assertEqual(tree.xpath('//button[@id="reading-toggle"]/text()'),['Show More (5 of 10)'])
        links=tree.xpath('//nav[@aria-label="Municipal Utilities"]/a[@href="/billing/utilities/26/generate-metsoa?billing_month=2026-10"]');self.assertEqual(len(links),1)
    def test_duplicate_conflicting_readings_do_not_produce_invented_consumption(self):
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(1,'2026-10-23',40)"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10')
        row=next(r for a in data['accounts'] for r in a['readings'] if r['number']=='CEL888')
        self.assertIsNone(row['difference']);self.assertIn('Conflicting readings',row['error'])

def load_tests(loader,tests,pattern):return unittest.TestSuite(MunicipalMetsoaTests(n) for n in MunicipalMetsoaTests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
