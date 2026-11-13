"""Short-number migration, identity preservation and real concurrent PostgreSQL inserts."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
import importlib.util
import re
import unittest
from uuid import uuid4
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from dotenv import dotenv_values
from lxml import html
import test_protrade_shop_register as base
import test_protrade_debtor_foundation as foundation


def migrate(connection):
    spec=importlib.util.spec_from_file_location('quote_numbers_migration',base.ROOT/'migrations/versions/protrade_quote_numbers_008.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.op=Operations(MigrationContext.configure(connection));module.upgrade();return module


class QuoteNumberingTests(unittest.TestCase):
    setUp=foundation.DebtorFoundationTests.setUp
    shop=foundation.DebtorFoundationTests.shop
    save_debtor=foundation.DebtorFoundationTests.save_debtor
    debtor=foundation.DebtorFoundationTests.debtor
    save_quote=foundation.DebtorFoundationTests.save_quote
    quote_url=foundation.DebtorFoundationTests.quote_url

    def apply(self):
        self.db_session.commit()
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN quotation_seq CASCADE')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN quotation_seq CASCADE')
        self.migration=migrate(self.conn);self.db_session.expire_all()

    def test_chronological_shared_backfill_preserves_every_original_column(self):
        self.legacy_quote.created_at=datetime(2020,1,1)
        customer=base.models.MechClient(user_id=1,shop_id=self.carpentry.id,name='Existing')
        self.db_session.add(customer);self.db_session.flush()
        vehicle=base.models.MechVehicle(client_id=customer.id,make='Old car');self.db_session.add(vehicle);self.db_session.flush()
        job=base.models.MechJobCard(shop_id=self.carpentry.id,vehicle_id=vehicle.id,job_number='EXISTING-INVOICE',status='Billed',created_at=datetime(2021,1,1))
        self.db_session.add(job);self.db_session.flush()
        invoice=base.catalog_globals['MechInvoice'](job_card_id=job.id,total=Decimal('123'),status='Unpaid')
        later=base.ProTradeBoqQuote(user_id=1,shop_id=self.carpentry.id,trade='carpentry',quote_number='CAR-Q-INTERNAL-UNCHANGED',client_name='Existing',items=[],grand_total=25,created_at=datetime(2022,1,1))
        unassigned=base.models.MechJobCard(vehicle_id=vehicle.id,job_number='LEGACY-NO-SHOP',status='Quote',created_at=datetime(2019,1,1))
        self.db_session.add_all([invoice,later,unassigned]);self.db_session.commit()
        before={}
        for table in ['protrade_boq_quotes','mech_job_cards','mech_invoices','protrade_boq_items','debtor']:
            columns=[c['name'] for c in sa.inspect(self.conn).get_columns(table) if c['name']!='quotation_seq']
            sql='SELECT '+','.join(columns)+' FROM '+table+' ORDER BY id'
            before[sql]=self.conn.exec_driver_sql(sql).all()
        self.apply()
        for sql,rows in before.items():self.assertEqual(self.conn.exec_driver_sql(sql).all(),rows)
        self.assertEqual(self.legacy_quote.display_quote_number,'Q-0001')
        self.assertEqual(job.display_quote_number,'Q-0002')
        self.assertEqual(later.display_quote_number,'Q-0003')
        self.assertEqual(job.display_document_number,'EXISTING-INVOICE')
        self.assertIsNone(unassigned.quotation_seq)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM protrade_quote_number').scalar(),3)
        with self.assertRaisesRegex(RuntimeError,'permanent'):self.migration.downgrade()

    def test_new_save_replay_edit_and_register_keep_uuid_internal(self):
        self.apply();self.save_debtor()
        first=self.save_quote();quote=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.debtor().id).one()
        internal=quote.quote_number;identity=quote.id
        self.assertEqual(quote.display_quote_number,'Q-0002')
        key=internal.removeprefix('CAR-Q-')
        self.save_quote(key=key,quantity='3')
        self.assertEqual((quote.id,quote.quote_number,quote.quotation_seq),(identity,internal,2))
        self.assertEqual(self.conn.exec_driver_sql('SELECT last_number FROM protrade_quote_counter WHERE shop_id='+str(self.carpentry.id)).scalar(),2)
        tree=html.fromstring(self.client.get(first.location).data)
        self.assertIn('Q-0002',tree.get_element_by_id('quote-current-reference').text_content())
        self.assertNotIn(internal,tree.get_element_by_id('debtor-quote-register').text_content())
        self.assertIn('Q-0002',tree.get_element_by_id('debtor-quote-register').text_content())
        self.assertEqual(tree.xpath('//input[@name="quote_key"]/@value'),[key.lower()])
        empty=self.shop(name='New independent shop',trade='carpentry',status='active')
        self.save_debtor(empty)
        response=self.client.post(self.quote_url(self.debtor(empty)),data={'quote_key':uuid4().hex,'item_id[]':['template:CAR-003'],'quantity_template:CAR-003':'1','material_rate_template:CAR-003':'259','labour_rate_template:CAR-003':'60'})
        self.assertEqual(response.status_code,302)
        other=base.ProTradeBoqQuote.query.filter_by(shop_id=empty.id).one()
        self.assertEqual(other.display_quote_number,'Q-0001')

    def test_orm_fetches_generated_number_on_flush_before_commit(self):
        self.apply()
        quote=base.ProTradeBoqQuote(user_id=1,shop_id=self.carpentry.id,trade='carpentry',quote_number='CAR-Q-'+uuid4().hex.upper(),client_name='Pending',items=[],grand_total=0)
        self.db_session.add(quote);self.db_session.flush()
        self.assertEqual(quote.quotation_seq,2)
        self.assertEqual(quote.display_quote_number,'Q-0002')

    def test_assigned_numbers_cannot_be_changed_or_reused_after_deletion(self):
        self.apply();self.save_debtor();self.save_quote()
        quote=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        with self.assertRaises(sa.exc.DBAPIError):
            with self.conn.begin_nested():self.conn.exec_driver_sql('UPDATE protrade_boq_quotes SET quotation_seq=99 WHERE id='+str(quote.id))
        self.db_session.delete(quote);self.db_session.commit()
        self.save_quote()
        self.assertEqual(base.ProTradeBoqQuote.query.filter_by(is_draft=True).one().quotation_seq,3)


class QuoteNumberConcurrencyTests(unittest.TestCase):
    def test_parallel_mixed_quote_inserts_unique_shop_sequences_and_rollback(self):
        url=dotenv_values(base.ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1)
        parsed=sa.engine.make_url(url);self.assertEqual(parsed.database,'ait_local_db');self.assertIn(parsed.host,('localhost','127.0.0.1','::1'))
        prefix='qn_'+uuid4().hex[:16]+'_'
        names=['mech_shops','protrade_boq_quotes','mech_job_cards','protrade_quote_counter','protrade_quote_number','protrade_assign_quote_number','uq_protrade_quote_document','uq_boq_shop_quote_number','uq_mechanic_shop_quote_number']
        engine=sa.create_engine(url)
        @sa.event.listens_for(engine,'before_cursor_execute',retval=True)
        def isolate(conn,cursor,statement,parameters,context,executemany):
            for name in names:statement=re.sub(r'(?<![\w])'+name+r'(?![\w])',prefix+name,statement)
            return statement,parameters
        try:
            with engine.begin() as c:
                self.assertEqual(c.execute(sa.text('SELECT current_database()')).scalar(),'ait_local_db')
                c.exec_driver_sql('CREATE TABLE mech_shops (id integer PRIMARY KEY)')
                c.exec_driver_sql('INSERT INTO mech_shops VALUES (1),(2)')
                for table in ['protrade_boq_quotes','mech_job_cards']:
                    c.exec_driver_sql('CREATE TABLE '+table+' (id serial PRIMARY KEY,shop_id integer NOT NULL,created_at timestamp DEFAULT now())')
                migrate(c)
            def create(i):
                table='protrade_boq_quotes' if i%2 else 'mech_job_cards'
                with engine.begin() as c:return c.exec_driver_sql('INSERT INTO '+table+'(shop_id) VALUES(1) RETURNING quotation_seq').scalar_one()
            with ThreadPoolExecutor(max_workers=8) as pool:numbers=list(pool.map(create,range(20)))
            self.assertEqual(sorted(numbers),list(range(1,21)))
            with engine.connect() as c:
                tx=c.begin();self.assertEqual(c.exec_driver_sql('INSERT INTO protrade_boq_quotes(shop_id) VALUES(1) RETURNING quotation_seq').scalar(),21);tx.rollback()
            with engine.begin() as c:
                self.assertEqual(c.exec_driver_sql('INSERT INTO mech_job_cards(shop_id) VALUES(1) RETURNING quotation_seq').scalar(),21)
                self.assertEqual(c.exec_driver_sql('INSERT INTO protrade_boq_quotes(shop_id) VALUES(2) RETURNING quotation_seq').scalar(),1)
                self.assertEqual(c.exec_driver_sql('SELECT count(*) FROM protrade_quote_number').scalar(),22)
        finally:
            assert re.fullmatch(r'qn_[0-9a-f]{16}_',prefix)
            with engine.begin() as c:
                for table in ['protrade_boq_quotes','mech_job_cards','protrade_quote_number','protrade_quote_counter','mech_shops']:
                    c.exec_driver_sql('DROP TABLE IF EXISTS '+table+' CASCADE')
                c.exec_driver_sql('DROP FUNCTION IF EXISTS protrade_assign_quote_number()')
            engine.dispose()


class QuoteNumberDocumentTests(unittest.TestCase):
    import test_protrade_output as fixtures
    setUp=fixtures.ProTradeOutputTests.setUp
    shop=fixtures.ProTradeOutputTests.shop
    contact=fixtures.ProTradeOutputTests.contact
    boq_quote=fixtures.ProTradeOutputTests.boq_quote
    preview=fixtures.ProTradeOutputTests.preview

    def test_output_customer_pdf_uses_short_quote_reference_and_invoice_keeps_original(self):
        self.db_session.commit()
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN quotation_seq CASCADE')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN quotation_seq CASCADE')
        migrate(self.conn);self.db_session.expire_all()
        preview=self.preview()
        self.assertEqual(preview.status_code,200)
        tree=html.fromstring(preview.data)
        document=tree.get_element_by_id('protrade-output-document').text_content()
        self.assertIn('Q-0001',document);self.assertNotIn(self.quote.quote_number,document)
        import json
        url=json.loads(tree.get_element_by_id('output-directory-context').text)['pdf_url']
        pdf=self.client.get(url);self.assertEqual(pdf.status_code,200)
        import fitz
        with fitz.open(stream=pdf.data,filetype='pdf') as rendered:
            text=''.join(page.get_text() for page in rendered)
            self.assertIn('Q-0001',text);self.assertNotIn(self.quote.quote_number,text)
            folder=base.ROOT/'tmp/pdfs';folder.mkdir(parents=True,exist_ok=True)
            (folder/'protrade-short-quote.pdf').write_bytes(pdf.data)
            rendered[0].get_pixmap(matrix=fitz.Matrix(1,1)).save(str(folder/'protrade-short-quote.png'))
        invoice_preview=self.preview(f'invoice:stored-{self.invoice.id}',self.mechanic)
        self.assertEqual(invoice_preview.status_code,200)
        invoice=html.fromstring(invoice_preview.data).get_element_by_id('protrade-output-document').text_content()
        self.assertIn('JOB-OUR',invoice)


if __name__=='__main__':unittest.main()
