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
        # New construction items are managed in the canonical price list (021),
        # rather than mutating or deleting the retained legacy flat rows.
        self.assertEqual(self.client.post('/mechanic/catalog',data=dict(shop_id=plumbing.id,category='plumbing',action='save_boq',description='Valve repair',unit='Each',material_rate='30',labour_rate='20')).status_code,302)
        canonical=base.ProTradeBoqItem.query.filter_by(shop_id=plumbing.id).one()
        code=canonical.code
        self.assertTrue(code.startswith('PLUM-C-'))
        self.assertEqual(self.client.post('/mechanic/catalog',data=dict(shop_id=plumbing.id,category='plumbing',action='save_boq',item_id=canonical.id,description='Valve repair updated',unit='Each',material_rate='40',labour_rate='20')).status_code,302)
        self.assertEqual(canonical.code,code)
        self.assertEqual(historic.items,old_items)
        self.assertEqual([part.item_code for part in parts],['PLUM-001','PLUM-002','PLUM-003'])
