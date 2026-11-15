"""Actual Paystack callbacks and fulfillment on isolated PostgreSQL tables."""
import ast
from copy import deepcopy
from datetime import datetime
import hashlib
import hmac
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4
from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from flask_login import current_user
import sqlalchemy as sa
import requests
import test_protrade_shop_register as base
from app.payments import paystack_security as security

node = next(node for node in ast.parse((base.ROOT/'app/models/payment.py').read_text()).body
    if isinstance(node, ast.ClassDef) and node.name == 'PaystackPayment')
namespace = dict(db=base.catalog_db, datetime=datetime)
exec(compile(ast.Module(body=[node], type_ignores=[]), 'production_payment_model', 'exec'), namespace)
Payment = namespace['PaystackPayment']


class PaystackSecurityTests(unittest.TestCase):
    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        self.app.jinja_loader.loaders[0].mapping['payments/success.html'] = 'Payment status'
        blueprint = Blueprint('paystack_bp', __name__)
        source = ast.parse((base.ROOT/'app/payments/paystack.py').read_text())
        names = {'success','webhook','fulfill_order','process_verified_payment','create_paystack_transaction'}
        nodes = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names]
        self.ns = dict(self.ns, User=base.models.User, PaystackPayment=Payment,
            AitTokenWallet=base.models.AitTokenWallet, AitTokenTransaction=base.TokenTransaction,
            paystack_bp=blueprint, csrf=SimpleNamespace(exempt=lambda function:function),
            get_paystack_secret=lambda subject=None:'sk_test_fixture', requests=requests,
            text=sa.text, os=os, hmac=hmac, hashlib=hashlib, json=json, datetime=datetime, uuid4=uuid4,
            jsonify=jsonify, current_app=current_app)
        tree = base.ProductionImports().visit(ast.Module(body=nodes,type_ignores=[]))
        exec(compile(ast.fix_missing_locations(tree),'production_paystack_routes','exec'),self.ns)
        self.app.register_blueprint(blueprint)
        self.app.add_url_rule('/start', 'paystack_bp.paystack_start', lambda:'checkout')

    def transaction(self, reference='payment-1', signed=True):
        metadata = dict(email='one@example.test', subject='mechanic_topup', tokens_purchased=100)
        if signed:
            metadata['order_token'] = security.signed_order(reference,'one@example.test','mechanic_topup',10000,'ZAR',100)
        return dict(reference=reference,status='success',amount=10000,currency='ZAR',domain='test',metadata=metadata)

    def response(self, transaction):
        return Mock(json=lambda:dict(status=True,data=deepcopy(transaction)))

    def webhook(self, transaction):
        body = json.dumps(dict(event='charge.success',data=transaction)).encode()
        signature = hmac.new(b'sk_test_fixture',body,hashlib.sha512).hexdigest()
        return self.client.post('/webhook',data=body,content_type='application/json',headers={'x-paystack-signature':signature})

    def test_redirect_identity_cannot_redirect_credit_and_webhook_replays_once(self):
        with self.app.app_context():
            transaction=self.transaction()
        balance=base.models.AitTokenWallet.query.one().balance
        with patch.object(security.requests,'get',return_value=self.response(transaction)):
            self.assertEqual(self.client.get('/success?reference=payment-1&email=two@example.test&subject=other_topup').status_code,200)
            for _ in range(3):
                self.assertEqual(self.webhook(transaction).status_code,200)
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance+100)
        self.assertEqual(base.TokenTransaction.query.count(),1)
        payment=Payment.query.one()
        self.assertEqual((payment.user_id,payment.subject_slug,payment.checkout_id),(1,'mechanic_topup','paystack:payment-1'))

    def test_historical_receipt_status_also_prevents_repeat_credit(self):
        self.db_session.add(Payment(user_id=1,email='one@example.test',subject_slug='mechanic_topup',
            amount_cents=10000,currency='ZAR',gateway_reference='payment-1',status='completed'))
        self.db_session.commit()
        balance=base.models.AitTokenWallet.query.one().balance
        with self.app.app_context():transaction=self.transaction()
        with patch.object(security.requests,'get',return_value=self.response(transaction)):
            self.assertEqual(self.webhook(transaction).status_code,200)
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance)
        self.assertEqual(base.TokenTransaction.query.count(),0)
        self.assertEqual(Payment.query.count(),1)

    def test_amount_currency_tokens_and_reference_tampering_rejected(self):
        with self.app.app_context():
            original=self.transaction()
            for change in ({'amount':500},{'currency':'USD'},{'reference':'another'},{'status':'failed'}):
                transaction=deepcopy(original)
                transaction.update(change)
                with patch.object(security.requests,'get',return_value=self.response(transaction)):
                    with self.assertRaises(ValueError):security.verified_transaction('payment-1','sk_test_fixture')
            transaction=deepcopy(original)
            transaction['metadata']['tokens_purchased']=100000
            with patch.object(security.requests,'get',return_value=self.response(transaction)):
                with self.assertRaises(ValueError):security.verified_transaction('payment-1','sk_test_fixture')
        self.assertEqual(Payment.query.count(),0)
        self.assertEqual(base.TokenTransaction.query.count(),0)

    def test_signature_and_gateway_failure_leave_wallet_unchanged_and_allow_retry(self):
        with self.app.app_context():transaction=self.transaction()
        balance=base.models.AitTokenWallet.query.one().balance
        self.assertEqual(self.client.post('/webhook',json=dict(event='charge.success',data=transaction)).status_code,401)
        with patch.object(security.requests,'get',side_effect=requests.Timeout()):
            self.assertEqual(self.webhook(transaction).status_code,503)
        self.assertEqual(Payment.query.count(),0)
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance)
        with patch.object(security.requests,'get',return_value=self.response(transaction)):
            self.assertEqual(self.webhook(transaction).status_code,200)
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance+100)

    def test_legacy_metadata_string_is_verified_and_test_money_rejected_on_render(self):
        with self.app.app_context():
            transaction=self.transaction(signed=False)
            transaction['metadata']=json.dumps(transaction['metadata'])
            with patch.object(security.requests,'get',return_value=self.response(transaction)):
                email,subject,verified=security.verified_transaction('payment-1','sk_test_fixture')
                self.assertEqual((email,subject,verified['metadata']['tokens_purchased']),('one@example.test','mechanic_topup',100))
            with patch.dict(os.environ,{'RENDER':'true'}), patch.object(security.requests,'get',return_value=self.response(transaction)):
                with self.assertRaises(ValueError):security.verified_transaction('payment-1','sk_test_fixture')

    def test_checkout_uses_server_quote_when_client_posts_invented_price_or_tokens(self):
        blueprint=Blueprint('payment_bp',__name__)
        source=ast.parse((base.ROOT/'app/payments/routes.py').read_text())
        node=next(node for node in source.body if isinstance(node,ast.FunctionDef) and node.name=='wallet_checkout')
        namespace=dict(self.ns,payment_bp=blueprint)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'production_wallet_checkout','exec'),namespace)
        self.app.register_blueprint(blueprint)
        with self.client.session_transaction() as cookie:
            cookie['wallet_quotes']={'mechanic':dict(user_id=1,tokens=100,amount_cents=10000,currency='ZAR')}
        result=self.client.post('/wallet/checkout',data=dict(subject_slug='mechanic',tokens='1000000',zar_price_cents='500'))
        self.assertEqual(result.status_code,302)
        with self.client.session_transaction() as cookie:
            self.assertEqual((cookie['topup_tokens'],cookie['topup_amount_cents']),(100,10000))



    def test_concurrent_callbacks_credit_once_and_distinct_payments_accumulate(self):
        from concurrent.futures import ThreadPoolExecutor
        from sqlalchemy.orm import scoped_session, sessionmaker
        import re
        prefix = 'ps_' + uuid4().hex[:10] + '_'
        engine = sa.create_engine(self.engine.url)
        names = set(base.models.Base.metadata.tables)
        for table in base.models.Base.metadata.tables.values():
            names.update(item.name for item in list(table.constraints) + list(table.indexes) if item.name)

        @sa.event.listens_for(engine, 'before_cursor_execute', retval=True)
        def isolate(connection, cursor, statement, parameters, context, executemany):
            for name in names:
                statement = re.sub(r'(?<![\w])' + re.escape(name) + r'(?![\w])', prefix + name, statement)
            return statement, parameters

        sessions = scoped_session(sessionmaker(bind=engine))
        try:
            base.models.Base.metadata.create_all(engine, checkfirst=False)
            sessions.add(base.models.User(id=1,name='Payment owner',email='one@example.test'))
            sessions.flush()
            sessions.add(base.models.AitTokenWallet(user_id=1,balance=0))
            sessions.commit()
            sessions.remove()
            with self.app.app_context():
                transactions={f'payment-{index}':self.transaction(f'payment-{index}') for index in range(5)}

            def verify(url, **kwargs):
                return self.response(transactions[url.rsplit('/',1)[-1]])

            def process(reference):
                with self.app.app_context():
                    try:
                        return self.ns['process_verified_payment'](reference,'mechanic_topup')
                    finally:
                        sessions.remove()

            with patch.dict(self.ns,db=SimpleNamespace(session=sessions)), patch.object(base.models.Base,'query',sessions.query_property()), patch.object(security.requests,'get',side_effect=verify):
                with ThreadPoolExecutor(max_workers=8) as workers:
                    results=list(workers.map(process,['payment-0']*8))
                self.assertEqual(sum(results),1)
                with ThreadPoolExecutor(max_workers=4) as workers:
                    self.assertTrue(all(workers.map(process,[f'payment-{index}' for index in range(1,5)])))
                self.assertEqual(sessions.query(base.models.AitTokenWallet).one().balance,500)
                self.assertEqual(sessions.query(Payment).count(),5)
                self.assertEqual(sessions.query(base.TokenTransaction).count(),5)
        finally:
            sessions.remove()
            with engine.begin() as connection:
                for table in reversed(base.models.Base.metadata.sorted_tables):
                    connection.execute(sa.schema.DropTable(table,if_exists=True))
            engine.dispose()

if __name__=='__main__':unittest.main()
