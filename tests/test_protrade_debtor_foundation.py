"""Stage 3H: shop debtor ownership and draft identity, real PostgreSQL."""
import importlib.util
import unittest
from decimal import Decimal
from uuid import uuid4
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_shop_register as base
from test_protrade_trade_quotes import TokenTransaction, InviteLog


class DebtorFoundationTests(unittest.TestCase):
    shop = base.ShopRegisterTests.shop

    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        self.carpentry = self.shop(name='Happy Shop',trade='carpentry',status='active')
        self.second = self.shop(name='Happy Shop',trade='generic',status='active')
        self.mechanic = self.shop(name='Mechanic',trade='mechanic',status='active')
        self.foreign = self.shop(owner=2,name='Private Shop',trade='carpentry',status='active')
        self.item = base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-004',description='Skirting',unit='m',material_rate=Decimal('85'),labour_rate=Decimal('45'))
        self.legacy_client = base.models.MechClient(user_id=1,name='Legacy person')
        self.db_session.add_all([self.item,self.legacy_client])
        self.db_session.flush()
        self.legacy_quote = base.ProTradeBoqQuote(user_id=1,shop_id=self.carpentry.id,client_id=self.legacy_client.id,trade='carpentry',quote_number='LEGACY-CAR',client_name='Legacy person',items=[],grand_total=Decimal('19.50'))
        self.db_session.add(self.legacy_quote)
        self.db_session.commit()
        spec=importlib.util.spec_from_file_location('debtor_migration',base.ROOT/'migrations/versions/protrade_debtor_006.py')
        self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
        self.migration.op=Operations(MigrationContext.configure(self.conn))
        self.migration.upgrade()

    def save_debtor(self,shop=None,expected=302,**changes):
        shop=shop or self.carpentry
        data=dict(name='Debtor Jane',phone='0123456789',email='jane@example.test',address='Shop customer address')
        data.update(changes)
        result=self.client.post(f'/mechanic/shop/{shop.id}/debtors',data=data)
        self.assertEqual(result.status_code,expected,result.get_data(as_text=True)[:250])
        return result

    def debtor(self,shop=None):
        return base.models.Debtor.query.filter_by(shop_id=(shop or self.carpentry).id).one()

    def quote_url(self,debtor=None):
        debtor=debtor or self.debtor()
        return f'/mechanic/shop/{debtor.shop_id}/debtor/{debtor.id}/quote'

    def save_quote(self,url=None,key=None,quantity='2.5',**changes):
        data={'quote_key':key or uuid4().hex,'item_id[]':[str(self.item.id)],f'quantity_{self.item.id}':quantity}
        data.update(changes)
        result=self.client.post(url or self.quote_url(),data=data)
        self.assertEqual(result.status_code,302,result.get_data(as_text=True)[:300])
        return result

    def test_debtors_capture_edit_permanent_shop_and_owner_links(self):
        self.save_debtor()
        debtor=self.debtor()
        self.assertEqual((debtor.user_id,debtor.shop_id,debtor.address),(1,self.carpentry.id,'Shop customer address'))
        client=self.db_session.get(base.models.MechClient,debtor.reference_id)
        self.assertEqual((client.shop_id,client.name),(self.carpentry.id,'Debtor Jane'))
        self.save_debtor(debtor_id=debtor.id,name='Jane Updated',address='Updated address')
        self.assertEqual(debtor.name,'Jane Updated')
        self.assertEqual(client.name,'Jane Updated')
        self.assertEqual(debtor.address,'Updated address')
        response=self.client.get(f'/mechanic/shop/{self.carpentry.id}/debtors')
        self.assertIn('Jane Updated',response.get_data(as_text=True))
        self.assertIn(self.quote_url(debtor),response.get_data(as_text=True))
        self.save_debtor(self.second)
        self.assertEqual(base.models.Debtor.query.count(),2)
        self.assertNotEqual(self.debtor(self.second).reference_id,debtor.reference_id)
        self.save_debtor(name='Jane Updated',address='Updated address')
        self.assertEqual(base.models.Debtor.query.count(),2)

    def test_debtor_isolation_and_saved_debtor_requirement(self):
        self.save_debtor()
        debtor=self.debtor()
        self.assertNotIn('Debtor Jane',self.client.get(f'/mechanic/shop/{self.second.id}/debtors').get_data(as_text=True))
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.foreign.id}/debtors').status_code,404)
        self.save_debtor(self.second,debtor_id=debtor.id,expected=404)
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.second.id}/debtor/{debtor.id}/quote').status_code,404)
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.carpentry.id}/debtor/999999/quote').status_code,404)
        old=self.client.get(f'/mechanic/shop/{self.carpentry.id}/quote')
        self.assertEqual(old.status_code,302)
        self.assertIn('/debtors',old.location)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),1)

    def test_reopen_save_edit_and_repeat_post_keep_one_draft_and_prices(self):
        self.save_debtor()
        url=self.quote_url()
        for _ in range(3):
            self.assertEqual(self.client.get(url).status_code,200)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),1)
        first=self.save_quote()
        draft=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        identifier=draft.id
        self.assertEqual((draft.debtor_id,draft.grand_total),(self.debtor().id,Decimal('325.00')))
        for _ in range(3):
            self.assertEqual(self.client.get(first.location).status_code,200)
            self.assertEqual(self.client.get(url).status_code,200)
        self.item.material_rate=Decimal('999')
        self.db_session.commit()
        self.save_quote(quantity='3')
        self.db_session.refresh(draft)
        self.assertEqual(draft.id,identifier)
        self.assertEqual(draft.grand_total,Decimal('390.00'))
        self.assertEqual(draft.items[0]['material_rate'],'85.00')
        self.save_quote(quantity='3')
        self.assertEqual(base.ProTradeBoqQuote.query.count(),2)
        self.assertEqual(self.legacy_quote.grand_total,Decimal('19.50'))
        self.assertIsNone(self.legacy_quote.debtor_id)
        self.assertIsNone(self.legacy_quote.is_draft)

    def test_contact_edit_does_not_rewrite_quote_until_explicit_draft_save(self):
        self.save_debtor();self.save_quote()
        draft=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        self.save_debtor(debtor_id=self.debtor().id,name='Updated Jane',email='updated@example.test')
        self.assertEqual(draft.client_name,'Debtor Jane')
        self.assertEqual(draft.client_email,'jane@example.test')
        self.assertEqual(self.client.get(self.quote_url()).status_code,200)
        self.assertEqual(draft.client_name,'Debtor Jane')
        self.save_quote()
        self.assertEqual(draft.client_name,'Updated Jane')
        self.assertEqual(draft.client_email,'updated@example.test')
        self.assertEqual(base.ProTradeBoqQuote.query.count(),2)

    def test_new_quote_is_explicit_and_keeps_previous_record(self):
        self.save_debtor();self.save_quote()
        first=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        old=(first.id,first.quote_number,first.items,first.grand_total)
        response=self.client.post(f'/mechanic/shop/{self.carpentry.id}/debtor/{self.debtor().id}/quotes/new')
        self.assertEqual(response.status_code,302)
        self.assertFalse(first.is_draft)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),2)
        self.assertEqual(self.client.get(response.location).status_code,200)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),2)
        self.save_quote()
        self.assertEqual(base.ProTradeBoqQuote.query.count(),3)
        self.assertEqual((first.id,first.quote_number,first.items,first.grand_total),old)
        self.assertEqual(base.ProTradeBoqQuote.query.filter_by(is_draft=True).count(),1)

    def test_db_uniqueness_and_additive_migration_preserve_legacy_records(self):
        before=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all()
        prices=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        self.migration.upgrade()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all(),before)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)
        self.save_debtor();self.save_quote()
        with self.conn.begin_nested() as tx:
            with self.assertRaises(sa.exc.DBAPIError):
                self.conn.exec_driver_sql("INSERT INTO protrade_boq_quotes (user_id,shop_id,debtor_id,is_draft,trade,quote_number,client_name,items,grand_total,created_at) SELECT user_id,shop_id,debtor_id,is_draft,trade,'SECOND-DRAFT',client_name,items,grand_total,created_at FROM protrade_boq_quotes WHERE is_draft IS TRUE")
            tx.rollback()

    def test_mechanic_quote_opens_and_edits_same_draft_without_repeat_charge(self):
        self.ns.update(text=sa.text,AitTokenTransaction=TokenTransaction,InviteLog=InviteLog)
        self.conn.exec_driver_sql('CREATE TEMP TABLE system_settings (key text PRIMARY KEY,value text)')
        wallet=base.models.AitTokenWallet(user_id=1,balance=50)
        self.db_session.add(wallet);self.db_session.commit()
        self.save_debtor(self.mechanic)
        debtor=self.debtor(self.mechanic)
        url=self.quote_url(debtor)
        opened=self.client.get(url)
        self.assertEqual(opened.status_code,302)
        self.assertIn('/mechanic/quote/new',opened.location)
        self.assertEqual(base.models.MechJobCard.query.count(),0)
        payload=dict(shop_id=self.mechanic.id,debtor_id=debtor.id,client_id=debtor.reference_id,customer_name=debtor.name,vehicle_reg='ABC123')
        created=self.client.post('/mechanic/quote/new',data=payload)
        self.assertEqual(created.status_code,302)
        draft=base.models.MechJobCard.query.one()
        self.assertEqual((draft.debtor_id,draft.shop_id,draft.is_draft),(debtor.id,self.mechanic.id,True))
        self.assertEqual(wallet.balance,45)
        for _ in range(2):
            reopened=self.client.get(url)
            self.assertIn('/edit',reopened.location)
            edit=self.client.get(reopened.location)
            self.assertEqual(edit.status_code,200)
            action=html.fromstring(edit.data).xpath('//form[@enctype="multipart/form-data"]/@action')[0]
            self.assertIn('/edit',action)
            self.assertEqual(self.client.post(action,data=payload).status_code,302)
            self.assertEqual(self.client.post('/mechanic/quote/new',data=payload).status_code,302)
        self.assertEqual(base.models.MechJobCard.query.count(),1)
        self.assertEqual(wallet.balance,45)
        self.client.post(f'/mechanic/shop/{self.mechanic.id}/debtor/{debtor.id}/quotes/new')
        self.assertFalse(draft.is_draft)
        self.assertEqual(base.models.MechJobCard.query.count(),1)
        self.client.post('/mechanic/quote/new',data=payload)
        self.assertEqual(base.models.MechJobCard.query.count(),2)
        self.assertEqual(wallet.balance,40)

    def test_migration_from_old_columns_preserves_existing_documents_and_prices(self):
        legacy=base.models.Debtor(user_id=1,name='Old unassigned debtor')
        self.db_session.add(legacy);self.db_session.commit()
        vehicle=base.models.MechVehicle(client_id=self.legacy_client.id,make='Old Car')
        self.db_session.add(vehicle);self.db_session.flush()
        job=base.models.MechJobCard(job_number='OLD-JOB',vehicle_id=vehicle.id,status='Quote',shop_id=self.mechanic.id)
        self.db_session.add(job);self.db_session.flush()
        invoice=base.catalog_globals['MechInvoice'](job_card_id=job.id,total=100,status='Unpaid')
        self.db_session.add(invoice);self.db_session.commit()
        excluded={'debtor':{'shop_id','address'},'mech_clients':{'shop_id'},'protrade_boq_quotes':{'debtor_id','is_draft'},'mech_job_cards':{'debtor_id','is_draft'}}
        snapshots={}
        for table in ('debtor','mech_clients','protrade_boq_quotes','mech_job_cards','mech_invoices','protrade_boq_items'):
            columns=','.join(column['name'] for column in sa.inspect(self.conn).get_columns(table) if column['name'] not in excluded.get(table,set()))
            snapshots[table]=(columns,self.conn.exec_driver_sql(f'SELECT {columns} FROM {table} ORDER BY id').all())
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN debtor_id CASCADE,DROP COLUMN is_draft')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN debtor_id CASCADE,DROP COLUMN is_draft')
        self.conn.exec_driver_sql('ALTER TABLE mech_clients DROP COLUMN shop_id')
        self.conn.exec_driver_sql('ALTER TABLE debtor DROP COLUMN shop_id,DROP COLUMN address')
        self.migration.upgrade()
        for table,(columns,records) in snapshots.items():
            self.assertEqual(self.conn.exec_driver_sql(f'SELECT {columns} FROM {table} ORDER BY id').all(),records)
        self.assertIsNone(self.conn.exec_driver_sql('SELECT shop_id FROM debtor').scalar())
        self.assertIsNone(self.conn.exec_driver_sql('SELECT is_draft FROM protrade_boq_quotes').scalar())

    def test_browser_debtor_capture_and_reopened_draft_fields(self):
        from playwright.sync_api import sync_playwright
        self.save_debtor();self.save_quote()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page()
                debtor=self.debtor()
                response=self.client.get(f'/mechanic/shop/{self.carpentry.id}/debtors?debtor_id={debtor.id}')
                page.set_content(response.get_data(as_text=True))
                self.assertEqual(page.locator('input[name=name]').input_value(),'Debtor Jane')
                self.assertEqual(page.locator('input[name=address]').input_value(),'Shop customer address')
                self.assertEqual(page.get_by_role('link',name='Quote',exact=True).count(),1)
                page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.evaluate("document.getElementById('quote-area-form').hidden=false")
                self.assertEqual(page.locator('input[name="item_id[]"]').input_value(),str(self.item.id))
                self.assertEqual(page.locator(f'input[name="quantity_{self.item.id}"]').input_value(),'2.5')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'325.00')
            finally:
                browser.close()

    def test_dashboard_is_shop_first_and_menu_has_only_foundation_actions(self):
        self.save_debtor()
        response=self.client.get(f'/mechanic/dashboard?shop_id={self.carpentry.id}')
        tree=html.fromstring(response.data)
        table=tree.get_element_by_id('client-register-table')
        self.assertEqual(len(table.xpath('.//td[1]/a[text()="Happy Shop"]')),2)
        self.assertNotIn('Debtor Jane',table.text_content())
        self.assertNotIn('Legacy person',table.text_content())
        self.assertTrue(table.xpath(f'.//a[@href="/mechanic/shop/{self.carpentry.id}/debtors"]'))
        source=(base.ROOT/'templates/program_mechanic/menu.html').read_text(encoding='utf-8')
        with self.app.test_request_context('/'):
            menu=self.app.jinja_env.from_string(source).render(active_shop=self.carpentry,protrade_menu_active='dashboard')
        self.assertEqual(html.fromstring(menu).xpath('//a/text()'),['Price List','Debtor','Tokens','Output'])


if __name__=='__main__':
    unittest.main(verbosity=2)
