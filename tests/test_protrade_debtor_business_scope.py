"""Same debtor names remain separate business accounts with scoped selection."""
from uuid import uuid4
import unittest
from lxml import html
import sqlalchemy as sa
import test_protrade_shop_register as base
import test_protrade_debtor_reuse as reuse
import test_protrade_boq_migration_compatibility as compatibility


class BusinessDebtorTests(unittest.TestCase):
    setUp = reuse.DebtorReuseTests.setUp
    shop = reuse.DebtorReuseTests.shop
    save_debtor = reuse.DebtorReuseTests.save_debtor
    debtor = reuse.DebtorReuseTests.debtor
    save_quote = reuse.DebtorReuseTests.save_quote
    quote_url = reuse.DebtorReuseTests.quote_url
    ledger_url = reuse.DebtorReuseTests.ledger_url
    fixtures = reuse.DebtorReuseTests.fixtures

    def register_url(self, shop):
        return f'/mechanic/shop/{shop.id}/debtors'

    def shared_accounts(self):
        self.fixtures()
        self.carpentry.business_name = 'Business A'
        self.second.business_name = 'Business B'
        self.db_session.commit()
        values = dict(name='Shared Customer',phone='0115550100',email='shared@example.test',address='Client address')
        for shop in (self.carpentry,self.second):
            for _ in range(2):
                self.assertEqual(self.client.post(self.register_url(shop),data=values).status_code,302)
        accounts = [base.models.Debtor.query.filter_by(shop_id=shop.id,name=values['name']).one() for shop in (self.carpentry,self.second)]
        return accounts

    def test_same_identification_across_businesses_and_scoped_search_selection(self):
        first, second = self.shared_accounts()
        self.assertNotEqual(first.id,second.id)
        self.assertNotEqual(first.reference_id,second.reference_id)
        self.assertEqual((first.identity_debtor_id,second.identity_debtor_id),(None,None))
        for shop, account, other in ((self.carpentry,first,second),(self.second,second,first)):
            page = html.fromstring(self.client.get(self.register_url(shop)).data)
            options = page.xpath('//datalist[@id="existing-debtor-options"]/option/@data-id')
            self.assertIn(str(account.id),options)
            self.assertNotIn(str(other.id),options)
            selected = self.client.post(self.register_url(shop),data={'existing_debtor_id':account.id})
            self.assertEqual(selected.status_code,302)
            self.assertIn('debtor_id='+str(account.id),selected.location)
            self.assertEqual(self.client.post(self.register_url(shop),data={'existing_debtor_id':other.id}).status_code,404)
        with self.client.session_transaction() as cookie:cookie['_user_id']='2'
        self.assertEqual(self.client.get(self.register_url(self.carpentry)).status_code,404)
        self.assertEqual(self.client.post(self.register_url(self.carpentry),data={'existing_debtor_id':first.id}).status_code,404)

    def test_duplicate_edit_and_legacy_null_contacts_use_existing_identification_rule(self):
        self.fixtures()
        self.tomas.phone=None
        self.tomas.email=None
        self.db_session.commit()
        count=base.models.Debtor.query.count()
        response=self.client.post(self.register_url(self.carpentry),data=dict(name='tOMAS',phone='',email='',address='Ignored duplicate'))
        self.assertEqual(response.status_code,302)
        self.assertEqual(base.models.Debtor.query.count(),count)
        self.assertEqual(self.tomas.address,'Shop customer address')
        self.save_debtor(name='Another client',phone='123',email='other@example.test')
        other=base.models.Debtor.query.filter_by(shop_id=self.carpentry.id,name='Another client').one()
        old=(other.name,other.phone,other.email,other.reference_id)
        response=self.client.post(self.register_url(self.carpentry),data=dict(debtor_id=other.id,name='Tomas',phone='',email=''))
        self.assertEqual(response.status_code,409)
        self.assertEqual((other.name,other.phone,other.email,other.reference_id),old)
        # The existing rule allows a distinct contact identity with the same name.
        self.assertEqual(self.client.post(self.register_url(self.carpentry),data=dict(name='Tomas',phone='different',email='')).status_code,302)

    def test_same_names_keep_quotes_payments_ledgers_and_statements_separate(self):
        first, second = self.shared_accounts()
        original=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        first_data={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(self.item.id)],f'quantity_{self.item.id}':'1'}
        second_data={'area':'new','quote_key':uuid4().hex,'item_id[]':['template:WORK-001'],
            'quantity_template:WORK-001':'1','material_rate_template:WORK-001':'200','labour_rate_template:WORK-001':'60'}
        for account,data in ((first,first_data),(second,second_data)):
            self.assertEqual(self.client.post(self.quote_url(account),data=data).status_code,302)
        first_quote=base.ProTradeBoqQuote.query.filter_by(debtor_id=first.id).one()
        second_quote=base.ProTradeBoqQuote.query.filter_by(debtor_id=second.id).one()
        self.assertEqual((first_quote.shop_id,first_quote.client_id),(first.shop_id,first.reference_id))
        self.assertEqual((second_quote.shop_id,second_quote.client_id),(second.shop_id,second.reference_id))
        for account,amount in ((first,'30'),(second,'60')):
            self.assertEqual(self.client.post(self.ledger_url(account)+'/add_payment',data=dict(amount=amount,date='2026-11-15',payment_method='Cash',payment_key=uuid4().hex,ref='SAME-REF')).status_code,302)
        totals=[]
        for account in (first,second):
            rows=base.DebtorLedger.query.filter_by(debtor_id=account.id).all()
            totals.append(sum(row.amount if row.kind=='debit' else -row.amount for row in rows))
            self.assertEqual(len(rows),2)
            self.assertEqual(self.client.post(f'/debtor/{account.id}/soa',data={'action':'generate'}).status_code,302)
            receipt=base.ProTradeDocument.query.filter_by(debtor_id=account.id,kind='soa').one()
            self.assertEqual(receipt.snapshot['issuer']['id'],account.shop_id)
            self.assertEqual(receipt.snapshot['debtor']['id'],account.id)
            self.assertEqual({row['id'] for row in receipt.snapshot['transactions']},{row.id for row in rows})
        self.assertEqual(totals,[10000,20000])
        self.assertEqual(self.client.get(self.ledger_url(second)+f'?shop_id={first.shop_id}').status_code,404)
        self.assertEqual(self.client.get(f'/mechanic/shop/{first.shop_id}/debtor/{second.id}/quote').status_code,404)
        self.assertEqual(self.client.post(self.ledger_url(second)+f'/add_payment?shop_id={first.shop_id}',data=dict(amount='1')).status_code,404)
        untouched=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger WHERE debtor_id NOT IN (%s,%s) ORDER BY id',(first.id,second.id)).all()
        self.assertEqual(untouched,original)

    def test_identical_mechanic_names_keep_invoice_on_its_own_account(self):
        compatibility.BoqMigrationCompatibilityTests.fixtures(self)
        self.ns.update(SenderProfile=base.models.SenderProfile)
        from test_protrade_trade_quotes import InviteLog, TokenTransaction
        self.ns.update(text=sa.text,InviteLog=InviteLog,AitTokenTransaction=TokenTransaction)
        self.conn.exec_driver_sql('CREATE TEMP TABLE system_settings(key text PRIMARY KEY,value text)')
        other_shop=self.shop(name='Second workshop',trade='mechanic',status='active')
        self.save_debtor(other_shop,name=self.customer.name,phone=self.customer.phone,email=self.customer.email)
        other=self.debtor(other_shop)
        before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger WHERE debtor_id=%s ORDER BY id',(self.customer.id,)).all()
        payload=dict(shop_id=other_shop.id,debtor_id=other.id,client_id=other.reference_id,customer_name=other.name,vehicle_reg='BUSINESS-B')
        self.assertEqual(self.client.post('/mechanic/quote/new',data=payload).status_code,302)
        quote=base.models.MechJobCard.query.filter_by(shop_id=other_shop.id).one()
        self.assertEqual(self.client.post(f'/mechanic/job/{quote.id}/approve',data={'pop_amount':'0'}).status_code,302)
        invoice=base.ProTradeDocument.query.filter_by(kind='invoice').one()
        self.assertEqual((invoice.shop_id,invoice.debtor_id),(other_shop.id,other.id))
        self.db_session.add(base.models.SenderProfile(user_id=1,business_name='Unrelated default business',is_default=True))
        self.db_session.commit()
        for account,shop in ((self.customer,self.mechanic),(other,other_shop)):
            self.assertEqual(self.client.post(f'/debtor/{account.id}/soa',data={'action':'generate'}).status_code,302)
            statement=base.ProTradeDocument.query.filter_by(kind='soa',debtor_id=account.id).one()
            self.assertEqual(statement.snapshot['issuer']['id'],shop.id)
            self.assertEqual(statement.snapshot['issuer']['business_name'],shop.business_name)
            self.assertEqual(statement.snapshot['debtor']['id'],account.id)
        self.assertEqual(self.job.status,'Quote')
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger WHERE debtor_id=%s ORDER BY id',(self.customer.id,)).all(),before)


if __name__=='__main__':unittest.main()
