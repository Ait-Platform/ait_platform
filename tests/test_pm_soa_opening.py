"""Opening balances, migrations and cross-connection PM reference guarantees."""
import concurrent.futures
import hashlib
import json
import threading
import uuid
import unittest
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import text, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import scoped_session, sessionmaker
from lxml import html
import test_pm_soa_ledger as fixture


class OpeningBalanceTests(unittest.TestCase):
    setUp = fixture.PMTenantLedgerTests.setUp
    path = fixture.PMTenantLedgerTests.path
    posting_form = fixture.PMTenantLedgerTests.posting_form
    submit = fixture.PMTenantLedgerTests.submit

    def test_debit_credit_zero_fixed_description_and_disabled_button(self):
        cases=[(26,11,'debit','250.25',Decimal('250.25'),'D0001'),
               (26,44,'credit','20.10',Decimal('-20.10'),'C0001'),
               (27,22,'zero','0',Decimal('0.00'),'D0002')]
        for prop,tenant,direction,amount,expected,reference in cases:
            data=self.posting_form('opening_balance',prop,tenant,direction=direction,
                amount=amount,description='ignored',notes='Opening reconciliation')
            result=self.submit(data,prop,tenant)
            self.assertEqual(result.status_code,302,result.text)
            row=self.session.execute(text('SELECT * FROM bil_tenant_ledger WHERE tenant_id=:t'),dict(t=tenant)).mappings().one()
            self.assertEqual(row['description'],'Opening Balance')
            self.assertEqual(row['amount'],expected);self.assertEqual(row['ref'],reference)
            self.assertEqual(row['posting_notes'],'Opening reconciliation')
            page=self.client.get(self.path(prop,tenant),headers=self.headers)
            tree=html.fromstring(page.data)
            self.assertEqual(len(tree.xpath('//button[@disabled][text()="Opening Balance"]')),1)
            self.assertIn('Opening reconciliation',page.text);self.assertIn(reference,page.text)
            self.assertIn('Outstanding balance: R '+format(expected,',.2f'),page.text)
            self.assertEqual(self.submit(data,prop,tenant).status_code,302) # identical retry
            self.assertEqual(self.submit(self.posting_form('opening_balance',prop,tenant,
                direction=direction,amount=amount),prop,tenant).status_code,409)

    def test_opening_is_tenant_wide_even_through_second_property(self):
        self.session.execute(text("UPDATE bil_tenant SET property_id=27 WHERE id=11"));self.session.commit()
        # Tenant 11 remains associated with property 26 by its saved draft.
        self.assertEqual(self.submit(self.posting_form('opening_balance',direction='debit')).status_code,302)
        result=self.submit(self.posting_form('opening_balance',27,11,direction='credit'),27,11)
        self.assertEqual(result.status_code,409,result.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)

    def test_opening_validation_and_database_backstop(self):
        for direction,amount in [('zero','1'),('debit','0'),('credit','0'),('refund','1'),('','1')]:
            self.assertEqual(self.submit(self.posting_form('opening_balance',direction=direction,amount=amount)).status_code,400)
        self.assertEqual(self.submit(self.posting_form('opening_balance',direction='zero',amount='0')).status_code,302)
        self.session.commit()
        with self.assertRaises(IntegrityError):
            self.session.execute(text("""INSERT INTO bil_tenant_ledger
                (tenant_id,description,kind,amount) VALUES(11,' opening balance ','charge',50)"""))
        self.session.rollback()
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)

    def test_description_dropdowns_and_non_editable_reference(self):
        from app.program_billing import lease_soa_posting as posting
        for kind,expected in [('charge',posting.DEBIT_DESCRIPTIONS),('payment',posting.CREDIT_DESCRIPTIONS)]:
            response=self.client.get(self.path()+'?posting='+kind,headers=self.headers)
            tree=html.fromstring(response.data)
            self.assertEqual(tree.xpath('//select[@name="description"]/option/text()')[1:],list(expected))
            self.assertFalse(tree.xpath('//input[@name="reference"]'))
            self.assertFalse(tree.xpath('//input[@readonly][@value="Generated automatically when saved"]'))
        for kind,description in [('charge','Refundable Security Deposit'),('payment','Refund Paid'),('payment','Rent Due')]:
            self.assertEqual(self.submit(self.posting_form(kind,description=description)).status_code,400)
        data=self.posting_form(reference='D1234',notes='Receipt ABC-123')
        self.assertEqual(self.submit(data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT ref FROM bil_tenant_ledger')).scalar(),'D0001')

    def test_all_three_forms_hide_notes_and_references_but_posted_references_display(self):
        for kind in ('opening_balance','charge','payment'):
            page=self.client.get(self.path()+'?posting='+kind,headers=self.headers)
            self.assertEqual(page.status_code,200,page.text)
            tree=html.fromstring(page.data)
            self.assertFalse(tree.xpath('//form//textarea[@name="notes"]'))
            self.assertFalse(tree.xpath('//form//*[contains(@name,"reference")]'))
            self.assertNotIn('Explanatory notes (optional)',page.text)
            self.assertNotIn('Generated automatically when saved',page.text)
            self.assertFalse(tree.xpath('//form//label[contains(.,"Reference")]'))
        self.assertEqual(self.submit(self.posting_form()).status_code,302)
        self.assertEqual(self.submit(self.posting_form('payment')).status_code,302)
        page=self.client.get(self.path(),headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertIn('D0001',' '.join(tree.xpath('//tbody//text()')))
        self.assertIn('C0001',' '.join(tree.xpath('//tbody//text()')))

    def test_reference_exhaustion_does_not_wrap_or_write(self):
        self.session.execute(text("SELECT setval('pm_ledger_debit_ref_seq',9999,true)"));self.session.commit()
        result=self.submit(self.posting_form())
        self.assertEqual(result.status_code,409,result.text);self.assertIn('reached 9999',result.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
        # Credit sequence is independent.
        self.assertEqual(self.submit(self.posting_form('payment')).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT ref FROM bil_tenant_ledger')).scalar(),'C0001')

    def test_preceding_release_idempotency_metadata_remains_effective(self):
        from app.program_billing import lease_soa_posting as posting
        data=self.posting_form(reference='OLD-REF')
        with self.app.test_request_context():nonce=posting.signer().loads(data['posting_key'])[4]
        payload=[self.user.id,26,11,'charge',data['txn_date'],'100.10','Rent Due','OLD-REF']
        digest=hashlib.sha256(json.dumps(payload,ensure_ascii=False).encode()).hexdigest()
        old_ref='PMPOST:'+nonce+':'+digest+' | OLD-REF'
        self.session.execute(text("""INSERT INTO bil_tenant_ledger
            (tenant_id,description,kind,amount,ref) VALUES(11,'Rent Due','charge',100.10,:ref)"""),dict(ref=old_ref));self.session.commit()
        self.assertEqual(self.submit(data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT ref FROM bil_tenant_ledger')).scalars().all(),[old_ref])

    def test_concurrent_openings_references_and_idempotency_on_real_postgresql(self):
        """Separate connections exercise migrated constraints and production allocation."""
        from app.program_billing import lease_soa_posting as posting
        name='pm_soa_test_'+uuid.uuid4().hex
        self.assertRegex(name,r'^pm_soa_test_[a-f0-9]{32}$')
        table='public.'+name
        debit='public.'+name+'_d'
        credit='public.'+name+'_c'
        self.session.commit()
        # Local test role permits public CREATE but not CREATE SCHEMA. Randomly
        # named fixtures copy the actual migration's indexes, never application rows.
        self.conn.execute(text('CREATE TABLE '+table+' (LIKE bil_tenant_ledger INCLUDING ALL)'))
        for seq in (debit,credit):
            self.conn.execute(text('CREATE SEQUENCE '+seq+' MINVALUE 1 MAXVALUE 9999 NO CYCLE'))
        self.conn.commit()
        workers=create_engine(self.engine.url)
        sessions=scoped_session(sessionmaker(bind=workers))
        db=type('WorkerDB',(),{'session':sessions})()
        def batch(forms):
            barrier=threading.Barrier(len(forms))
            def run(form):
                try:
                    barrier.wait(timeout=10)
                    ref=posting.next_reference(form['direction'])
                    sessions.execute(text('INSERT INTO '+table+"""
                        (tenant_id,description,kind,amount,ref,posting_key)
                        VALUES(11,:description,:kind,:amount,:ref,:key)"""),dict(
                        description='Opening Balance' if form['kind']=='opening_balance' else 'Test posting',
                        kind=form['kind'],amount=Decimal('-10.10') if form['direction']=='credit' else Decimal('10.10'),
                        ref=ref,key=form['key']))
                    sessions.commit()
                    return 201
                except IntegrityError:
                    sessions.rollback()
                    return 409
                finally:sessions.remove()
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(forms)) as executor:
                return list(executor.map(run,forms))
        def form(kind,direction):
            return dict(kind=kind,direction=direction,key=uuid.uuid4().hex)
        try:
            with patch.object(posting,'db',db),patch.dict(posting.REFERENCE_SEQUENCES,{'D':debit,'C':credit}):
                self.assertEqual(sorted(batch([form('opening_balance','debit'),form('opening_balance','credit')])),[201,409])
                self.assertEqual(batch([form('charge','debit') for _ in range(4)]+[form('payment','credit') for _ in range(4)]),[201]*8)
                repeated=form('charge','debit')
                self.assertEqual(sorted(batch([repeated,repeated])),[201,409])
            with workers.connect() as c:
                rows=c.execute(text('SELECT ref,amount,kind FROM '+table)).all()
                self.assertEqual(len(rows),10)
                self.assertEqual(len({row.ref for row in rows}),10)
                self.assertTrue(all(__import__('re').fullmatch('[DC][0-9]{4}',row.ref) for row in rows))
                self.assertTrue(all((row.amount<0)==row.ref.startswith('C') for row in rows))
                self.assertEqual(sum(row.kind=='opening_balance' for row in rows),1)
        finally:
            sessions.remove();workers.dispose()
            # Only these validated randomly named local fixtures are removed.
            self.conn.execute(text('DROP TABLE '+table))
            for seq in (debit,credit):self.conn.execute(text('DROP SEQUENCE '+seq))
            self.conn.commit()


class PostingMigrationTests(unittest.TestCase):
    setUp = fixture.PMTenantLedgerTests.setUp
    def test_migration_preserves_rows_and_continues_historical_references(self):
        # Recreate only the disposable fixture ledger in its session-temp schema.
        self.session.close()
        self.conn.execute(text('DROP TABLE bil_tenant_ledger'))
        self.conn.execute(text('DROP SEQUENCE pm_ledger_debit_ref_seq,pm_ledger_credit_ref_seq'))
        self.conn.execute(text("""CREATE TEMP TABLE bil_tenant_ledger(
            id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,tenant_id integer NOT NULL,
            txn_date timestamp,month varchar(7),description varchar(255),kind varchar(50),amount numeric(10,2),ref varchar(255))"""))
        self.conn.execute(text("""INSERT INTO bil_tenant_ledger(tenant_id,description,kind,amount,ref)
            VALUES(11,'Historical charge','charge',100,'D0042'),(22,'Historical credit','payment',-10,'C0007'),
            (44,'Opening Balance','charge',0,'Historical opening')"""))
        before=self.conn.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).all()
        fixture.apply_posting_migration(self.conn);self.conn.commit()
        after=self.conn.execute(text('SELECT id,tenant_id,txn_date,month,description,kind,amount,ref FROM bil_tenant_ledger ORDER BY id')).all()
        self.assertEqual(before,after)
        self.assertEqual(self.conn.execute(text("SELECT nextval('pm_ledger_debit_ref_seq')")).scalar(),43)
        self.assertEqual(self.conn.execute(text("SELECT nextval('pm_ledger_credit_ref_seq')")).scalar(),8)

    def test_duplicate_historical_openings_block_migration_without_edits(self):
        self.session.execute(text('DROP INDEX uq_pm_ledger_opening'))
        self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,description,kind,amount) VALUES(11,'Opening Balance','charge',10),(11,' opening balance ','charge',20)"))
        self.session.commit()
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).all()
        with self.assertRaisesRegex(RuntimeError,'Duplicate historical opening balances'):
            fixture.apply_posting_migration(self.conn)
        self.conn.rollback()
        self.assertEqual(before,self.session.execute(text('SELECT * FROM bil_tenant_ledger ORDER BY id')).all())
