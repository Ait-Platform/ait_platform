"""Existing ledger connection: PostgreSQL ownership, balances and unchanged history."""
import re
from datetime import date
from lxml import html
import test_protrade_shop_register as base
from test_protrade_debtor_foundation import DebtorFoundationTests
import unittest

class DebtorLedgerLinkTests(unittest.TestCase):
    setUp=DebtorFoundationTests.setUp
    shop=DebtorFoundationTests.shop
    save_debtor=DebtorFoundationTests.save_debtor
    debtor=DebtorFoundationTests.debtor

    def test_link_existing_account_and_preserve_history(self):
        for endpoint in set(re.findall(r"url_for\('([^']+)'",(base.ROOT/'templates/program_mechanic/client_ledger.html').read_text())):
            if endpoint not in self.app.view_functions:self.app.add_url_rule('/stub/'+endpoint,endpoint,lambda **kw:'stub')
        from unittest.mock import patch
        from sqlalchemy.orm import Query
        from flask import abort
        def first_or_404(query):
            result=query.first()
            if result is None:abort(404)
            return result
        patcher=patch.object(Query,'first_or_404',first_or_404,create=True);patcher.start();self.addCleanup(patcher.stop)
        self.save_debtor(name='Tomas');debtor=self.debtor()
        self.save_debtor(self.second,name='Tomas');other=self.debtor(self.second)
        self.db_session.add_all([base.DebtorLedger(debtor_id=debtor.id,txn_date=date(2026,11,13),ref='INV-EXISTING',description='Carpentry services',kind='debit',amount=120000),base.DebtorLedger(debtor_id=debtor.id,txn_date=date(2026,11,14),ref='DEP-EXISTING',description='Deposit received',kind='credit',amount=50000),base.DebtorLedger(debtor_id=other.id,txn_date=date(2026,11,14),ref='OTHER-SHOP',description='Private',kind='debit',amount=99900)])
        jobs=[]
        for owner_debtor,shop in ((debtor,self.carpentry),(other,self.second)):
            vehicle=base.models.MechVehicle(client_id=owner_debtor.reference_id,make='Existing',model='Vehicle')
            self.db_session.add(vehicle);self.db_session.flush()
            job=base.models.MechJobCard(vehicle_id=vehicle.id,shop_id=shop.id,debtor_id=owner_debtor.id,job_number='SHOP-JOB-'+str(shop.id),status='Approved')
            self.db_session.add(job);jobs.append(job)
        self.db_session.commit()
        tree=html.fromstring(self.client.get(f'/mechanic/shop/{self.carpentry.id}/debtors').data)
        link=tree.xpath('//table[@id="shop-debtors"]//a[text()="Ledger"]')[0].get('href')
        self.assertIn(f'/client_ledger/{debtor.id}',link)
        before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        response=self.client.get(link);self.assertEqual(response.status_code,200)
        text=html.fromstring(response.data).text_content()
        self.assertIn('INV-EXISTING',text);self.assertIn('DEP-EXISTING',text);self.assertIn('700.00',text)
        self.assertNotIn('OTHER-SHOP',text)
        self.assertNotIn(jobs[0].job_number,text);self.assertNotIn(jobs[1].job_number,text)
        self.assertEqual(self.client.get(f'/mechanic/client_ledger/{debtor.id}?shop_id={self.second.id}').status_code,404)
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.client.get(link).status_code,404)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)
        with self.client.session_transaction() as session:session['_user_id']='1'
        payment=f'/mechanic/client_ledger/{debtor.id}/add_payment'
        self.assertEqual(self.client.post(payment+f'?shop_id={self.second.id}',data={'amount':'5'}).status_code,404)
        from uuid import uuid4
        self.assertEqual(self.client.post(payment,data={'amount':'5','date':'2026-11-14','ref':'EXISTING-PAYMENT-FLOW','payment_key':uuid4().hex}).status_code,302)
        self.assertEqual(jobs[0].status,'Approved');self.assertEqual(jobs[1].status,'Approved')
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=other.id).count(),1)

if __name__=='__main__':unittest.main()
