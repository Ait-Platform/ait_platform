"""Lease foundation on temporary local PostgreSQL fixtures; no production writes."""
import unittest,types,hashlib,tempfile
from pathlib import Path
from unittest.mock import patch
import sqlalchemy as sa
from sqlalchemy import event
import test_pm_owner_operations as owners
import test_pm_property_management as properties

class LeaseFoundationTests(unittest.TestCase):
    load_lease_workspace=True
    sign_in=properties.PropertyManagementTests.sign_in
    sql=owners.OwnerOperationsTests.sql
    assign=owners.OwnerOperationsTests.assign
    payload=owners.OwnerOperationsTests.payload
    def setUp(self):
        owners.OwnerOperationsTests.setUp(self)
        self.sql('ALTER TABLE "user" ADD COLUMN is_active integer DEFAULT 1')
        self.sql('CREATE TEMP TABLE bil_lease(id integer PRIMARY KEY,property_id integer,start_date text,end_date text,rent_amount numeric)')
        self.sql("INSERT INTO bil_lease VALUES(501,26,'2026-01-01','2026-12-31',1000),(601,27,'2026-02-01','2027-02-01',2000)")
        self.session.commit()

    def test_selection_authorization_owners_and_vertical_sections(self):
        self.assign(name='Persisted Owner',identity_number='LEASE-OWNER')
        response=self.client.get('/billing/leases?property_id=26')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        for text in ('>Lease</h1>','1. Select Property','R01 Lease snapshot','Part 2 Schedule 1 Lease Particulars','Part 3 Schedule 2 Special Conditions','Part 4 Supporting Annexures','Almond','Original address','Persisted Owner','501'):
            self.assertIn(text,html)
        self.assertNotIn('Private address',html);self.assertNotIn('601',html)
        self.assertNotIn('>Property &amp; Owner</a>',html.split('</nav>')[-1])
        self.assertIn('Persistent lease draft storage is not approved',html)
        self.assertEqual(self.client.post('/billing/leases',data={'reference':'Must not create'}).status_code,405)
        self.assertEqual(self.client.get('/billing/leases?property_id=27').status_code,404)
        for value in ('bad','0','-1'):
            self.assertEqual(self.client.get('/billing/leases?property_id='+value).status_code,404)
        self.sql('UPDATE "user" SET is_active=0 WHERE id=7');self.session.commit()
        self.assertEqual(self.client.get('/billing/leases').status_code,403)

    def test_missing_optional_column_is_identified_and_not_queried(self):
        self.sql('ALTER TABLE bil_lease DROP COLUMN rent_amount');self.session.commit()
        statements=[]
        def capture(conn,cursor,statement,parameters,context,many):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:
            with self.assertLogs(self.app.logger,level='WARNING') as logs:
                response=self.client.get('/billing/leases?property_id=26')
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn('bil_lease.rent_amount',html);self.assertIn('501',html)
        self.assertIn('bil_lease.rent_amount',''.join(logs.output))
        query=next(q for q in statements if 'FROM bil_lease JOIN bil_property' in q)
        self.assertNotIn('bil_lease.rent_amount',query)
        self.assertNotIn('bil_tenant',query);self.assertNotIn('bil_meter',query)

    def test_missing_property_link_fails_closed_without_lease_disclosure(self):
        self.sql('ALTER TABLE bil_lease DROP COLUMN property_id');self.session.commit()
        response=self.client.get('/billing/leases?property_id=26')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn('missing property authorization linkage',html)
        self.assertNotIn('>501</td>',html);self.assertNotIn('>601</td>',html)

    def test_undefined_column_after_inspection_rollback_keeps_selection_usable(self):
        self.assign(name='Owner After Error',identity_number='ERR-OWNER')
        original=self.session.execute
        def failing(statement,*args,**kwargs):
            if 'FROM bil_lease JOIN bil_property' in str(statement):
                return original(sa.text('SELECT disappeared_column FROM bil_lease'))
            return original(statement,*args,**kwargs)
        with patch.object(self.session,'execute',side_effect=failing):
            with self.assertLogs(self.app.logger,level='ERROR') as logs:
                response=self.client.get('/billing/leases?property_id=26')
        self.assertEqual(response.status_code,200)
        self.assertIn('Owner After Error',response.get_data(as_text=True))
        self.assertIn('sqlstate=42703',''.join(logs.output))
        self.assertIn('missing_column=disappeared_column',''.join(logs.output))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)

    def test_explicit_r01_association_and_private_document_integrity(self):
        import sys
        docs=sys.modules['app.program_billing.lease_documents']
        with tempfile.TemporaryDirectory(dir=properties.ROOT/'tmp') as folder:
            root=Path(folder);(root/'R01').mkdir()
            payloads={docs.PDF_NAME:b'%PDF-synthetic-review',docs.WORD_NAME:b'synthetic-docx'}
            for name,content in payloads.items():(root/'R01'/name).write_bytes(content)
            self.app.config.update(PM_LEASE_DOCUMENT_ROOT=str(root))
            with patch.object(docs,'EXPECTED_SHA256',{n:hashlib.sha256(b).hexdigest() for n,b in payloads.items()}):
                # Neither property name nor logged-in user infers the association.
                self.assertEqual(self.client.get('/billing/leases/preview?property_id=26').status_code,404)
                self.app.config['PM_LEASE_REVIEW_PROPERTY_ID']='26'
                self.assertEqual(self.client.get('/billing/leases/preview?property_id=26').data,payloads[docs.PDF_NAME])
                self.assertEqual(self.client.get('/billing/leases/preview?property_id=28').status_code,404)
                self.sign_in(8)
                self.assertEqual(self.client.get('/billing/leases/word?property_id=26').status_code,404)
                self.sign_in(7)
                (root/'R01'/docs.PDF_NAME).write_bytes(b'tampered')
                self.assertEqual(self.client.get('/billing/leases/preview?property_id=26').status_code,503)
                self.assertEqual(self.client.get('/billing/leases?property_id=26').status_code,200)

if __name__=='__main__':unittest.main()
