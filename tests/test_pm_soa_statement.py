"""Tenant-facing statement arithmetic and privacy on isolated PostgreSQL."""
import unittest,json
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from lxml import html
from sqlalchemy import text,event
import test_pm_soa_branding as fixture
from test_pm_soa_corrections import apply_corrections


class TenantStatementTests(unittest.TestCase):
    path=fixture.SOABrandingTests.path
    hub=fixture.SOABrandingTests.hub
    form_data=fixture.SOABrandingTests.form_data
    post_brand=fixture.SOABrandingTests.post_brand
    def setUp(self):
        fixture.SOABrandingTests.setUp(self)
        apply_corrections(self.conn);self.conn.commit()
        from app.program_billing import lease_soa_statement
        clock=patch.object(lease_soa_statement,'today',return_value=date(2026,10,29));clock.start();self.addCleanup(clock.stop)
        self.projection=lease_soa_statement
        self.session.execute(text("""INSERT INTO bil_tenant_ledger(tenant_id,txn_date,description,kind,amount,ref,posting_notes) VALUES
            (11,'2026-09-30','Rent Due','charge',1000,'D0010','PRIVATE AUDIT NOTES'),
            (11,'2026-10-01','Rent Due','charge',500,'D0011','SECRET USER 644'),
            (11,'2026-10-20','Payment Received','payment',-200,'C0010',NULL),
            (11,'2026-11-01','Future Charge','charge',999,'D0012',NULL),
            (22,'2026-10-01','Other tenant private debt','charge',9999,'D0013',NULL)"""));self.session.commit()
    def statement_url(self,p=26,t=11):return '/manager/tenant_accounts/'+str(p)+'/tenant/'+str(t)+'/statement'
    def test_default_month_opening_period_and_closing_balances(self):
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).all();queries=[]
        def capture(conn,cursor,statement,params,context,many):queries.append(statement.lower())
        event.listen(self.conn,'before_cursor_execute',capture)
        try:page=self.client.get(self.statement_url(),headers=self.headers)
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//input[@name="month"]/@value'),['2026-10'])
        self.assertIn('R 1,300.00',tree.xpath('string(//*[@id="statement-closing"])'))
        self.assertIn('1,000.00',tree.xpath('string(//tbody/tr[1])'))
        self.assertIn('D0011',page.text);self.assertIn('C0010',page.text)
        self.assertNotIn('D0010',page.text);self.assertNotIn('Future Charge',page.text);self.assertNotIn('Other tenant private debt',page.text)
        self.assertFalse(any(q.lstrip().startswith(('insert ','update ','delete ')) for q in queries))
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).all(),before)
        self.assertIn('private',page.headers['Cache-Control'])
    def test_custom_range_empty_month_and_invalid_dates(self):
        page=self.client.get(self.statement_url()+'?month=2026-10&from=2026-10-15&to=2026-10-31',headers=self.headers)
        tree=html.fromstring(page.data);self.assertEqual(page.status_code,200)
        self.assertIn('1,500.00',tree.xpath('string(//tbody/tr[1])'));self.assertIn('R 1,300.00',page.text)
        self.assertNotIn('D0011',page.text);self.assertIn('C0010',page.text)
        empty=self.client.get(self.statement_url()+'?month=2026-12',headers=self.headers)
        self.assertIn('No transactions in the selected period.',empty.text);self.assertIn('R 2,299.00',empty.text)
        for query in ('month=bad','month=2026-13','month=2026-10&from=2026-10-30&to=2026-10-01','month=2026-10&from=2026-02-30'):
            self.assertEqual(self.client.get(self.statement_url()+'?'+query,headers=self.headers).status_code,400)
    def test_corrected_rows_retain_references_but_never_expose_audit_data(self):
        self.session.execute(text("""INSERT INTO bil_tenant_ledger(tenant_id,txn_date,description,kind,amount,ref,correction_of,correction_role,correction_actor,correction_recorded_at,correction_reason,posting_key,correction_group)
            VALUES(11,'2026-10-21','Rent Due','correction_reversal',-500,'C0020',2,'reversal',644,now(),'PRIVATE CORRECTION REASON','11111111111111111111111111111111','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
            (11,'2026-10-21','Rent Due','correction_replacement',450,'D0020',2,'replacement',644,now(),'PRIVATE CORRECTION REASON','22222222222222222222222222222222','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"""));self.session.commit()
        page=self.client.get(self.statement_url(),headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        for private in ('PRIVATE CORRECTION REASON','PRIVATE AUDIT NOTES','SECRET USER 644','correction_actor','correction_recorded_at'):
            self.assertNotIn(private,page.text)
        self.assertIn('Reversal',page.text);self.assertIn('C0020',page.text);self.assertIn('D0020',page.text);self.assertIn('R 1,250.00',page.text)
    def test_branding_property_details_and_print_controls(self):
        self.assertEqual(self.post_brand(self.form_data(business_name='Selected SOA Brand')).status_code,302)
        identifier=self.session.execute(text('SELECT id FROM bil_pm_soa_branding')).scalar()
        self.assertEqual(self.post_brand(dict(csrf_token=self.csrf,action='select',kind='letterhead',id=str(identifier))).status_code,302)
        page=self.client.get(self.statement_url(),headers=self.headers)
        self.assertIn('Selected SOA Brand',page.text);self.assertIn('Synthetic property',page.text);self.assertIn('Natural Lessee',page.text)
        self.assertLess(page.text.index('Selected SOA Brand'),page.text.index('id="tenant-statement-transactions"'))
        tree=html.fromstring(page.data)
        self.assertTrue(tree.xpath('//button[@data-print-statement]'))
        self.assertFalse(tree.xpath('//form[@method="post"]|//a[text()="Edit"]'))
        self.assertIn('pm_soa_statement.css',page.text);self.assertIn('pm_soa_statement.js',page.text)
    def test_existing_permissions_read_only_and_missing_effective_date(self):
        self.assertEqual(self.client.get(self.statement_url()).status_code,403)
        for p,t in ((28,33),(27,11),(26,22)):
            self.assertEqual(self.client.get(self.statement_url(p,t),headers=self.headers).status_code,404)
        self.assertEqual(self.client.post(self.statement_url(),data=dict(csrf_token=self.csrf),headers=self.headers).status_code,405)
        self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,description,kind,amount) VALUES(11,'Undated historical entry','charge',1)"));self.session.commit()
        response=self.client.get(self.statement_url(),headers=self.headers)
        self.assertEqual(response.status_code,409);self.assertIn('no effective date',response.text)
