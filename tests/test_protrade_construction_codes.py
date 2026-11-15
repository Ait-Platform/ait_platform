"""Sequential construction codes reuse existing catalogue rows and preserve documents."""
import importlib.util
from copy import deepcopy
from uuid import uuid4
import unittest
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base


class ConstructionCodeTests(unittest.TestCase):
    setUp=boq.BoqTradeTests.setUp
    shop=boq.BoqTradeTests.shop
    save_debtor=boq.BoqTradeTests.save_debtor
    debtor=boq.BoqTradeTests.debtor
    save_quote=boq.BoqTradeTests.save_quote
    quote_url=boq.BoqTradeTests.quote_url
    ledger_url=boq.BoqTradeTests.ledger_url

    def test_migration_sequential_codes_ordinary_quotes_and_historical_references(self):
        boq.BoqTradeTests.fixtures(self)
        plumbing=self.shop(name='Plumbing contractor',trade='plumbing')
        self.save_debtor(plumbing,name='Plumbing customer')
        debtor=self.debtor(plumbing)
        for name in ('Repair tap','Replace pipe','Install basin'):
            self.db_session.add(base.MechCatalogPart(user_id=1,shop_id=plumbing.id,trade='plumbing',part_name=name,category='Work',default_price=80))
        old_items=[dict(source_item_id='template:WORK-001',code='WORK-001',description='Historical work',unit='Each',quantity='1',material_rate='100.00',labour_rate='0.00',total='100.00')]
        historic=base.ProTradeBoqQuote(user_id=1,shop_id=plumbing.id,debtor_id=debtor.id,client_id=debtor.reference_id,trade='plumbing',quote_number='CAR-Q-'+uuid4().hex.upper(),is_draft=False,client_name=debtor.name,items=old_items,grand_total=100)
        self.db_session.add(historic);self.db_session.commit()
        before=self.conn.exec_driver_sql('SELECT id,user_id,shop_id,trade,part_name,category,default_price,created_at FROM mech_catalog_parts ORDER BY id').all()
        ledger_before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        self.conn.exec_driver_sql('ALTER TABLE mech_catalog_parts DROP COLUMN item_code CASCADE')
        spec=importlib.util.spec_from_file_location('codes_migration',base.ROOT/'migrations/versions/protrade_trade_codes_019.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.conn));migration.upgrade()
        self.db_session.expire_all();self.db_session.commit()
        self.assertEqual(self.conn.exec_driver_sql('SELECT id,user_id,shop_id,trade,part_name,category,default_price,created_at FROM mech_catalog_parts ORDER BY id').all(),before)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),ledger_before)
        self.assertEqual(historic.items,old_items)
        parts=base.MechCatalogPart.query.filter_by(shop_id=plumbing.id).order_by(base.MechCatalogPart.id).all()
        self.assertEqual([part.item_code for part in parts],['PLUM-001','PLUM-002','PLUM-003'])
        url=f'/mechanic/catalog?shop_id={plumbing.id}'
        self.assertEqual(self.client.post(url,data=dict(shop_id=plumbing.id,action='add',part_name='Valve repair',price='50')).status_code,302)
        fourth=base.MechCatalogPart.query.filter_by(shop_id=plumbing.id,part_name='Valve repair').one()
        self.assertEqual(fourth.item_code,'PLUM-004')
        response=self.client.get(self.quote_url(debtor),query_string={'area':'new'})
        self.assertEqual(response.status_code,200)
        self.assertIn('PLUM-004',response.get_data(as_text=True))
        self.assertNotIn('WORK-001',response.get_data(as_text=True).split('quote-builder-data')[1])
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[f'part:{fourth.id}'],f'quantity_part:{fourth.id}':'2',f'description_part:{fourth.id}':'Adjusted plumbing description'}
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        saved=base.ProTradeBoqQuote.query.filter_by(shop_id=plumbing.id,is_draft=True).one()
        self.assertEqual(saved.items[0]['code'],'PLUM-004')
        self.assertEqual(saved.items[0]['description'],'Adjusted plumbing description')
        self.assertEqual(saved.items[0]['source_item_id'],f'part:{fourth.id}')
        self.assertEqual(fourth.part_name,'Valve repair')
        self.assertEqual(saved.grand_total,100)
        self.assertEqual(historic.items,old_items)
        self.assertEqual(self.client.post(url,data=dict(shop_id=plumbing.id,action='delete',part_id=fourth.id)).status_code,302)
        self.assertEqual(self.client.post(url,data=dict(shop_id=plumbing.id,action='add',part_name='New plumbing work',price='70')).status_code,302)
        self.assertEqual(base.MechCatalogPart.query.filter_by(shop_id=plumbing.id,part_name='New plumbing work').one().item_code,'PLUM-005')
