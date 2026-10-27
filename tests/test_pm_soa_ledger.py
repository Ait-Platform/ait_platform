"""Read-only PM ledger foundation on isolated PostgreSQL, no shared debtors."""
import unittest,json
from unittest.mock import patch
from sqlalchemy import text,event
from lxml import html
import test_pm_soa_recent as fixture

class PMTenantLedgerTests(unittest.TestCase):
    def setUp(self):
        fixture.RecentSOATests.setUp(self)
        self.session.execute(text('CREATE TEMP TABLE bil_tenant_ledger(id integer PRIMARY KEY,tenant_id integer,txn_date timestamp,description text,kind text,amount numeric(10,2))'))
        self.session.commit()
        from app.program_billing import lease_soa_ledger
        scoped=patch.object(lease_soa_ledger,'db',self.db);scoped.start();self.addCleanup(scoped.stop)
    def path(self,property_id=26,tenant_id=11):return '/manager/tenant_accounts/'+str(property_id)+'/tenant/'+str(tenant_id)+'/ledger'
    def test_open_ledger_empty_registered_account_and_read_only_methods(self):
        listing=self.client.get(self.url+'?property_id=26',headers=self.headers);tree=html.fromstring(listing.data)
        actions=tree.xpath('//tbody/tr[td[contains(.,"Natural Lessee")]]/td[last()]/a')
        self.assertEqual(len(actions),1);self.assertEqual(actions[0].text_content(),'Open Ledger →');self.assertEqual(actions[0].get('href'),self.path())
        response=self.client.get(self.path(),headers=self.headers);self.assertEqual(response.status_code,200,response.text)
        self.assertIn('Synthetic property',response.text);self.assertIn('Natural Lessee',response.text);self.assertIn('PM tenant account 11',response.text)
        self.assertIn('No transactions recorded for this tenant.',response.text);self.assertNotIn('<form',response.text)
        self.assertEqual(self.client.post(self.path(),headers=self.headers).status_code,405)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
    def test_signed_rand_transactions_chronology_exact_running_balances_and_no_writes(self):
        self.session.execute(text("""INSERT INTO bil_tenant_ledger VALUES
        (3,11,'2026-10-03','Payment received','payment',-25.20),
        (1,11,'2026-10-01','Monthly rental','charge',100.10),
        (2,11,'2026-10-02','Recoverable municipal charge','charge',20.20),
        (4,11,'2026-10-04','Credit adjustment','adjustment',-5.10),
        (5,22,'2026-10-01','Other tenant debt','charge',9999.00)"""));self.session.commit()
        before=list(self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).mappings());statements=[]
        def record(conn,cursor,statement,parameters,context,executemany):statements.append(statement.lower())
        event.listen(self.conn,'before_cursor_execute',record)
        try:response=self.client.get(self.path(),headers=self.headers)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200,response.text);tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//thead/tr/th/text()'),['Date','Description','Debit','Credit','Balance'])
        rows=tree.xpath('//tbody/tr');self.assertEqual([row[1].text_content() for row in rows],['Monthly rental','Recoverable municipal charge','Payment received','Credit adjustment'])
        self.assertEqual([row[4].text_content() for row in rows],['100.10','120.30','95.10','90.00'])
        self.assertEqual(rows[2][3].text_content(),'25.20');self.assertNotIn('Other tenant debt',response.text)
        self.assertTrue(all(sql.lstrip().startswith('select') for sql in statements))
        self.assertFalse(any('debtor' in sql or 'invoice' in sql or 'job_card' in sql for sql in statements))
        self.assertEqual(before,list(self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).mappings()))
    def test_property_manager_tenant_isolation_and_missing_schema(self):
        self.assertEqual(self.client.get(self.path()).status_code,403)
        self.assertEqual(self.client.get(self.path(28,33),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.path(26,22),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.path(26,33),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.path(27,22),headers=self.headers).status_code,200)
        self.session.execute(text('ALTER TABLE bil_tenant_ledger DROP COLUMN amount'));self.session.commit()
        response=self.client.get(self.path(),headers=self.headers);self.assertEqual(response.status_code,503);self.assertIn('schema is unavailable',response.text)
    def test_almond_marelise_draft_only_ledger_no_sample_transactions(self):
        self.user.id=644
        self.session.execute(text("UPDATE bil_property SET manager_id=644,name='Almond' WHERE id=26"))
        self.session.execute(text("INSERT INTO bil_tenant(id,manager_id,property_id,party_type,name,identity_number) VALUES (12,644,NULL,'natural_person','Marelise van der Merwe','TEST-12')"))
        self.session.execute(text('UPDATE bil_lease_draft SET manager_id=644,items=CAST(:items AS jsonb)'),dict(items=json.dumps({'_parties':{'lessees':[{'tenant_id':12}]}})));self.session.commit()
        page=self.client.get(self.path(26,12),headers=self.headers);self.assertEqual(page.status_code,200,page.text)
        self.assertIn('Almond',page.text);self.assertIn('Marelise van der Merwe',page.text);self.assertIn('PM tenant account 12',page.text)
        self.assertIn('No transactions recorded for this tenant.',page.text)
