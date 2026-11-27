"""Unified construction pricing: real routes, PostgreSQL migration and Chromium."""
from copy import deepcopy
from decimal import Decimal
import importlib.util
import json
from uuid import uuid4
import unittest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base


class UnifiedPricesTests(unittest.TestCase):
    setUp=boq.BoqTradeTests.setUp
    shop=boq.BoqTradeTests.shop
    save_debtor=boq.BoqTradeTests.save_debtor
    debtor=boq.BoqTradeTests.debtor
    save_quote=boq.BoqTradeTests.save_quote
    quote_url=boq.BoqTradeTests.quote_url
    ledger_url=boq.BoqTradeTests.ledger_url
    fixtures=boq.BoqTradeTests.fixtures

    def save_price(self,shop,category,description='Client work',**extra):
        data=dict(shop_id=shop.id,category=category,action='save_boq',description=description,unit='Each',material_rate='12.34',labour_rate='6.66')
        data.update(extra)
        return self.client.post('/mechanic/catalog',data=data)

    def choices(self,debtor):
        response=self.client.get(self.quote_url(debtor),query_string={'area':'new'})
        self.assertEqual(response.status_code,200)
        return response,json.loads(html.fromstring(response.data).get_element_by_id('quote-builder-data').text)

    def test_shared_management_defaults_general_works_and_client_isolation(self):
        for trade in ('plumbing','painting','carpentry','electrical','building','general'):
            with self.subTest(trade=trade):
                shop=self.shop(trade=trade)
                response=self.client.get('/mechanic/catalog',query_string={'shop_id':shop.id})
                self.assertEqual(response.status_code,200)
                tree=html.fromstring(response.data)
                self.assertEqual(tree.xpath('//select[@id="price-category"]/option[@selected]/@value'),[trade])
                self.assertIn('General Works',tree.text_content())
                self.assertEqual(tree.xpath('//table[@id="boq-items"]/thead/tr/th/text()'),['Item Code','Description','Unit','Material Rate','Labour Rate','Total Rate','Action'])
                self.assertEqual(base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).count(),0)
                self.assertEqual(self.save_price(shop,trade).status_code,302)
                row=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).one()
                self.assertEqual(row.total_rate,Decimal('19.00'))
                code=row.code
                self.assertEqual(self.save_price(shop,trade,item_id=row.id,description='Changed',material_rate='20').status_code,302)
                self.assertEqual(row.code,code)
                self.assertEqual(self.save_price(shop,trade,description='Changed').status_code,400)
        other=self.shop(trade='plumbing')
        private=self.shop(owner=2,trade='plumbing')
        row=base.ProTradeBoqItem.query.filter_by(trade='plumbing').first()
        self.assertEqual(self.save_price(other,'plumbing',item_id=row.id).status_code,404)
        self.assertEqual(self.save_price(private,'plumbing').status_code,404)
        self.assertEqual(self.save_price(other,'painting').status_code,404)

    def test_boq_uses_own_category_rates_and_rejects_same_owner_other_client(self):
        shop,debtor=self.fixtures()
        source=self.shop(trade='plumbing',name='Other plumbing client')
        self.assertEqual(self.save_price(source,'plumbing',description='Private client work',material_rate='999').status_code,302)
        foreign=base.ProTradeBoqItem.query.filter_by(shop_id=source.id).one()
        for category in ('plumbing','painting','general'):
            self.assertEqual(self.save_price(shop,category,description=category+' work').status_code,302)
        own=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).order_by(base.ProTradeBoqItem.id).all()
        response,data=self.choices(debtor)
        self.assertNotIn('Private client work',response.get_data(as_text=True))
        self.assertEqual({choice['source_id'] for choice in data['choices'] if isinstance(choice['source_id'],int)},{row.id for row in own})
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(row.id) for row in own]}
        payload.update({f'quantity_{row.id}':'2' for row in own})
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.grand_total,Decimal('114.00'))
        before=deepcopy(quote.items)
        row=own[0]
        self.assertEqual(self.save_price(shop,row.trade,item_id=row.id,description=row.description,material_rate='50').status_code,302)
        self.db_session.refresh(quote)
        self.assertEqual(quote.items,before)
        payload.update(quote_key=uuid4().hex)
        payload['item_id[]']=[str(foreign.id)];payload[f'quantity_{foreign.id}']='1'
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,400)

    def test_individual_quote_uses_own_trade_and_general_rates_without_updating_prices(self):
        boq_shop,_=self.fixtures()
        shop=self.shop(trade='plumbing',name='Independent plumbing client')
        self.save_debtor(shop,name='Plumbing debtor')
        debtor=self.debtor(shop)
        self.assertEqual(self.save_price(shop,'plumbing',description='Own pipe work',unit='m',material_rate='10',labour_rate='5').status_code,302)
        self.assertEqual(self.save_price(shop,'general',description='Own general work',material_rate='4',labour_rate='3').status_code,302)
        self.assertEqual(self.save_price(boq_shop,'plumbing',description='Another client rate',material_rate='999').status_code,302)
        response,data=self.choices(debtor)
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//select[@id="quote-catalogue"]/option[@selected]/@value'),['plumbing'])
        self.assertFalse(tree.xpath('//select[@id="quote-catalogue"]/@multiple'))
        self.assertNotIn('Another client rate',response.get_data(as_text=True))
        pipe=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id,trade='plumbing').one()
        general=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id,trade='general').one()
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(pipe.id),str(general.id)],f'quantity_{pipe.id}':'1.5',f'material_rate_{pipe.id}':'11',f'quantity_{general.id}':'1'}
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.grand_total,Decimal('31.00'))
        self.assertEqual(pipe.material_rate,Decimal('10.00'))
        self.assertEqual({row['trade'] for row in quote.items},{'plumbing','general'})

    def test_browser_categories_autofill_quantity_and_exact_combined_calculation(self):
        shop,debtor=self.fixtures()
        self.assertEqual(self.save_price(shop,'plumbing',description='Pipe repair').status_code,302)
        combined=base.ProTradeBoqItem(user_id=1,shop_id=shop.id,trade='painting',code='PAINT-OLD',description='Legacy finish',unit='m',combined_rate=Decimal('19.995'),material_rate=None,labour_rate=None)
        self.db_session.add(combined);self.db_session.commit()
        response,data=self.choices(debtor)
        pipe=next(row for row in data['choices'] if row['description']=='Pipe repair')
        old=next(row for row in data['choices'] if row['code']=='PAINT-OLD')
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.locator('#quote-catalogue').select_option(['plumbing','painting'])
                page.locator('#quote-work-select').fill(pipe['label'])
                self.assertEqual(page.locator('#quote-work-quantity').input_value(),'1')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'12.34')
                page.locator('#quote-add-item').click()
                page.locator('#quote-work-select').fill(old['label'])
                self.assertTrue(page.locator('#quote-work-material').is_disabled())
                self.assertEqual(page.locator('#quote-work-combined').input_value(),'19.995')
                page.locator('#quote-work-quantity').fill('3')
                self.assertEqual(page.locator('#quote-work-total').text_content(),'59.99')
                page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'78.99')
                self.assertEqual(page.locator('[name="combined_rate_'+str(combined.id)+'"]').input_value(),'19.995')
            finally:
                browser.close()
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(combined.id)],f'quantity_{combined.id}':'3'}
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.items[0]['combined_rate'],'19.995')
        self.assertEqual(quote.items[0]['material_rate'],'')
        self.assertEqual(quote.grand_total,Decimal('59.99'))
        # Output uses the same canonical list; item edits retain the unsplit basis.
        from flask_login import login_user
        import app.protrade_output as output
        with self.app.test_request_context():
            login_user(self.db_session.get(base.models.User,1))
            report=output.report_data(shop,dict(key='price_list:all',kind='price_list',label='Price List',source=None))
        old_row=next(row for row in report['document']['rows'] if row[0]=='PAINT-OLD')
        self.assertEqual(old_row[3:5],[chr(8212),chr(8212)])
        self.assertIn('19.995 (combined)',old_row[5])
        item_id=self.ns['carpentry_quote_item_id'](quote,quote.items[0])
        url=f'/mechanic/shop/{shop.id}/debtor/{debtor.id}/quotes/{quote.id}/items'
        response=self.client.get(url,query_string={'item_id':item_id})
        tree=html.fromstring(response.data)
        payload={node.name:node.value for node in tree.xpath('//form[@id="quote-item-edit"]//input[@name]')}
        payload.update(unit='m',quantity='2',combined_rate='19.995')
        self.assertEqual(self.client.post(url,data=payload).status_code,302)
        self.db_session.refresh(quote)
        self.assertEqual(quote.grand_total,Decimal('39.99'))
        self.assertEqual(quote.items[0]['combined_rate'],'19.995')
        self.assertEqual(combined.combined_rate,Decimal('19.995'))


    def test_migration_preserves_legacy_prices_ids_snapshots_and_mechanic(self):
        shop,debtor=self.fixtures()
        plumbing=self.shop(trade='plumbing')
        part=base.MechCatalogPart(user_id=1,shop_id=plumbing.id,trade='plumbing',item_code='PLUM-OLD',part_name='Historic pipe',category='Work',default_price=19.995)
        self.db_session.add(part);self.db_session.commit()
        tables=('mech_catalog_parts','protrade_boq_quotes','mech_invoices','debtor_ledger','ait_token_wallets')
        before={table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables}
        # Recreate the pre-021 item schema using real production revisions.
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_items')
        self.conn.exec_driver_sql('CREATE TEMP TABLE IF NOT EXISTS "user" (id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES (1),(2) ON CONFLICT DO NOTHING')
        for filename in ('protrade_boq_002.py','protrade_boq_003.py','protrade_boq_004.py'):
            self.run_migration(filename)
        self.conn.exec_driver_sql("INSERT INTO protrade_boq_items(user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,quantity,created_at) VALUES (1,"+str(shop.id)+",'boq','OLD-1','Original item','Each',12.34,5.66,2,now())")
        original=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').mappings().all()
        # A code collision must roll back all DDL and inserts, never merge prices.
        import sqlalchemy as sa
        with self.conn.begin_nested() as conflict:
            self.conn.exec_driver_sql("INSERT INTO protrade_boq_items(user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,quantity,created_at) VALUES (1,"+str(plumbing.id)+",'plumbing','PLUM-OLD','Unrelated existing item','Each',1,2,1,now())")
            with self.conn.begin_nested() as attempt:
                with self.assertRaises(sa.exc.IntegrityError):
                    self.run_migration('protrade_unified_prices_021.py')
                attempt.rollback()
            fields=[row[0] for row in self.conn.exec_driver_sql("SELECT a.attname FROM pg_attribute a WHERE a.attrelid='protrade_boq_items'::regclass AND a.attnum>0 AND NOT a.attisdropped")]
            self.assertNotIn('combined_rate',fields)
            conflict.rollback()
        from alembic import command
        from alembic.config import Config
        self.conn.exec_driver_sql('CREATE TEMP TABLE alembic_version(version_num varchar(32) PRIMARY KEY)')
        self.conn.exec_driver_sql("INSERT INTO alembic_version VALUES ('protrade_trade_codes_019'),('pm_bank_id_003')")
        config=Config(str(base.ROOT/'migrations/alembic.ini'))
        config.set_main_option('script_location',str(base.ROOT/'migrations'))
        config.attributes.update(connection=self.conn,skip_app_metadata=True)
        command.upgrade(config,'protrade_unified_prices_021')
        expected={'protrade_unified_prices_021','pm_bank_id_003'}
        self.assertEqual(set(self.conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalars()),expected)
        command.upgrade(config,'protrade_unified_prices_021')
        self.assertEqual(set(self.conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalars()),expected)
        after=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items WHERE legacy_part_id IS NULL').mappings().all()
        for key,value in original[0].items():
            self.assertEqual(after[0][key],value,key)
        for table,rows in before.items():
            self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all(),rows,table)
        migrated=self.conn.exec_driver_sql('SELECT code,combined_rate,material_rate,labour_rate,legacy_part_id FROM protrade_boq_items WHERE legacy_part_id IS NOT NULL').one()
        self.assertEqual(tuple(migrated),('PLUM-OLD',Decimal('19.995'),None,None,part.id))
        self.db_session.expire_all()
        response=self.client.get('/mechanic/catalog',query_string={'shop_id':plumbing.id})
        self.assertIn('19.995',response.get_data(as_text=True))
        self.assertIn('Combined legacy rate',response.get_data(as_text=True))
        item=base.ProTradeBoqItem.query.filter_by(legacy_part_id=part.id).one()
        edit=self.client.get('/mechanic/catalog',query_string={'shop_id':plumbing.id,'edit_id':item.id})
        self.assertIn('replace_combined',edit.get_data(as_text=True))
        self.assertEqual(self.save_price(plumbing,'plumbing',description=item.description,item_id=item.id,replace_combined='yes',material_rate='15',labour_rate='5').status_code,302)
        self.db_session.refresh(item)
        self.assertIsNone(item.combined_rate)
        self.assertEqual(item.total_rate,Decimal('20.00'))
        self.assertEqual(part.default_price,19.995)


    def run_migration(self,filename):
        spec=importlib.util.spec_from_file_location('migration_'+filename,base.ROOT/'migrations/versions'/filename)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        module.op=Operations(MigrationContext.configure(self.conn));module.upgrade()
