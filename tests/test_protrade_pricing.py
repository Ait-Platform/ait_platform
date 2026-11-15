"""Approved prices are prospective, audited and isolated from Paystack fulfillment."""
from copy import deepcopy
import importlib.util
import unittest
from uuid import uuid4
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base
import app.protrade_pricing as pricing
import app.protrade_documents as documents


class ProTradePricingTests(unittest.TestCase):
    setUp=boq.BoqTradeTests.setUp
    shop=boq.BoqTradeTests.shop
    save_debtor=boq.BoqTradeTests.save_debtor
    debtor=boq.BoqTradeTests.debtor
    save_quote=boq.BoqTradeTests.save_quote
    quote_url=boq.BoqTradeTests.quote_url
    ledger_url=boq.BoqTradeTests.ledger_url

    def prepare(self):
        self.boq,self.account=boq.BoqTradeTests.fixtures(self)
        self.conn.exec_driver_sql('DROP TABLE protrade_pricing_revision')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES(1),(2)')
        self.conn.exec_driver_sql('DROP TABLE protrade_document')
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN boq_sections')
        for name in ['protrade_document_tokens_013','protrade_pricing_017']:
            spec=importlib.util.spec_from_file_location(name,base.ROOT/'migrations/versions'/f'{name}.py')
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            module.op=Operations(MigrationContext.configure(self.conn));module.upgrade()
        self.db_session.expire_all()
        self.db_session.commit()

    def approval(self,**changes):
        with self.app.test_request_context():
            values=pricing.current_prices()
        payload=dict(revision=base.ProTradePricingRevision.query.order_by(base.ProTradePricingRevision.id.desc()).first().id,
            reason='Approved prospective pricing review')
        payload.update({'tokens_'+key:str(value) for key,value in values['tokens'].items()})
        payload.update({'suggested_'+key:str(value/100) for key,value in values['suggested'].items()})
        payload.update({key:str(value/100) for key,value in values['boq'].items()})
        payload.update(changes)
        return payload

    def admin(self):
        with self.client.session_transaction() as session:session['is_admin']=True

    def test_admin_only_prices_history_and_prospective_receipts(self):
        self.prepare()
        url='/mechanic/admin/pricing'
        self.assertEqual(self.client.get(url).status_code,403)
        self.assertEqual(self.client.post(url,data=self.approval(tokens_quote='12')).status_code,403)
        with self.app.test_request_context():
            old=documents.charge_document(1,self.carpentry.id,self.tomas.id,'test-old','quote');self.db_session.commit()
        self.assertEqual(old.tokens,10)
        self.admin()
        self.assertEqual(self.client.get(url).status_code,200)
        self.assertEqual(self.client.post(url,data=self.approval(tokens_quote='12',suggested_quote_electronic='22.50')).status_code,302)
        self.assertEqual(base.ProTradePricingRevision.query.count(),2)
        revision=base.ProTradePricingRevision.query.order_by(base.ProTradePricingRevision.id.desc()).first()
        self.assertEqual(revision.user_id,1)
        self.assertEqual(revision.values['tokens']['quote'],12)
        self.assertEqual(revision.values['boq'],pricing.DEFAULTS['boq'])
        with self.app.test_request_context():
            old_again=documents.charge_document(1,self.carpentry.id,self.tomas.id,'test-old','quote')
            new=documents.charge_document(1,self.carpentry.id,self.tomas.id,'test-new','quote');self.db_session.commit()
        self.assertEqual(old_again.id,old.id)
        self.assertEqual((old.tokens,new.tokens),(10,12))
        self.assertEqual(base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance,978)
        response=self.client.get('/mechanic/help',query_string={'shop_id':self.boq.id})
        self.assertEqual(response.status_code,200)
        self.assertIn('12 tokens',response.get_data(as_text=True))
        self.assertIn('R22.50',response.get_data(as_text=True))
        for sql in ["UPDATE protrade_pricing_revision SET reason='tampered'", "DELETE FROM protrade_pricing_revision",
            "UPDATE universal_token_tariff SET base_token_cost=99 WHERE program_slug='protrade' AND action_name='quote'",
            "DELETE FROM universal_token_tariff WHERE program_slug='protrade'",
            "UPDATE protrade_document SET tokens=1 WHERE document_key='test-old'"]:
            with self.subTest(sql=sql), self.assertRaises(sa.exc.DBAPIError):
                with self.conn.begin_nested():self.conn.exec_driver_sql(sql)

    def test_boq_change_requires_explicit_consistent_joint_approval(self):
        self.prepare();self.admin();url='/mechanic/admin/pricing'
        ledger_before=[(row.id,row.amount,row.debtor_id) for row in base.DebtorLedger.query.all()]
        original=deepcopy(pricing.current_prices())
        self.assertEqual(self.client.post(url,data=self.approval(tokens_boq_section='30')).status_code,400)
        self.assertEqual(self.client.post(url,data=self.approval(tokens_boq_section='30',approve_boq_terms='yes')).status_code,400)
        self.assertEqual(pricing.current_prices(),original)
        stale=self.approval(tokens_quote='11')
        approved=self.approval(tokens_boq_section='30',contractor_fee_cents='55',approve_boq_terms='yes')
        self.assertEqual(self.client.post(url,data=approved).status_code,302)
        self.assertEqual(self.client.post(url,data=stale).status_code,409)
        with self.app.test_request_context():
            receipt=documents.charge_document(1,self.boq.id,self.account.id,'boq-section:test:'+uuid4().hex,'boq_section',snapshot={'section':{'name':'Work'}})
            self.db_session.commit()
        self.assertEqual(receipt.tokens,30)
        self.assertEqual(receipt.snapshot['commercial_terms']['contractor_fee_cents'],5500)
        self.assertEqual(receipt.snapshot['commercial_terms']['operator_retains_cents'],2500)
        self.assertEqual(receipt.snapshot['commercial_terms']['ait_wallet_value_cents'],3000)
        self.assertEqual([(row.id,row.amount,row.debtor_id) for row in base.DebtorLedger.query.all()],ledger_before)
