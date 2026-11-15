"""Client work history, retry protection and database audit guards."""
import ast
from datetime import date, datetime
import importlib.util
from uuid import uuid4
import unittest
from unittest.mock import patch
from flask import abort
from sqlalchemy.orm import Query
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_shop_register as base
import app.protrade_activity as activity

node=next(node for node in ast.parse((base.ROOT/'app/models/mechanic.py').read_text()).body
    if isinstance(node,ast.ClassDef) and node.name=='ProTradeWorkActivity')
namespace=dict(db=base.catalog_db,datetime=datetime)
exec(compile(ast.Module(body=[node],type_ignores=[]),'production_work_activity_model','exec'),namespace)
Activity=namespace['ProTradeWorkActivity']


class WorkActivityTests(unittest.TestCase):
    shop=base.ShopRegisterTests.shop

    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        def first_or_404(query):
            row=query.first()
            if row is None:abort(404)
            return row
        patcher=patch.object(Query,'first_or_404',first_or_404,create=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        activity.db=self.ns['db']
        activity.MechShop=base.models.MechShop
        activity.Debtor=base.models.Debtor
        activity.ProTradeWorkActivity=Activity
        self.ours=self.shop(trade='plumbing')
        self.second=self.shop(name='Second business',trade='electrical')
        self.foreign=self.shop(owner=2,name='Other owner')
        self.client_account=base.models.Debtor(user_id=1,shop_id=self.ours.id,name='Client Jane')
        self.other_account=base.models.Debtor(user_id=1,shop_id=self.second.id,name='Other account')
        self.db_session.add_all([self.client_account,self.other_account])
        self.db_session.commit()
        self.url=f'/mechanic/shop/{self.ours.id}/debtor/{self.client_account.id}/activity'
        self.conn.exec_driver_sql('DROP TABLE protrade_work_activity')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES(1),(2)')
        spec=importlib.util.spec_from_file_location('work_activity_migration',base.ROOT/'migrations/versions/protrade_work_activity_015.py')
        migration=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.conn))
        migration.upgrade()

    def test_record_correct_repeat_and_read_preserve_audit_and_wallet(self):
        balance=base.models.AitTokenWallet.query.one().balance
        key=uuid4().hex
        data=dict(entry_key=key,work_date='2026-11-14',description='Replaced leaking tap')
        for _ in range(3):self.assertEqual(self.client.post(self.url,data=data).status_code,302)
        original=Activity.query.one()
        correction=dict(previous_id=original.id,work_date='2026-11-13',description='Replaced leaking kitchen tap')
        for _ in range(3):self.assertEqual(self.client.post(self.url,data=correction).status_code,302)
        records=Activity.query.order_by(Activity.revision).all()
        self.assertEqual(len(records),2)
        self.assertEqual((records[0].work_date,records[0].description),(date(2026,11,14),'Replaced leaking tap'))
        self.assertEqual((records[1].revision,records[1].previous_id,records[1].entry_key),(2,original.id,key))
        page=self.client.get(self.url)
        self.assertEqual(page.status_code,200)
        for value in ('Replaced leaking tap','Replaced leaking kitchen tap','Audit history','2026-11-13'):self.assertIn(value,page.get_data(as_text=True))
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance)
        self.assertEqual(base.TokenTransaction.query.count(),0)
        for sql in ("UPDATE protrade_work_activity SET description='changed'", 'DELETE FROM protrade_work_activity'):
            with self.conn.begin_nested() as transaction:
                with self.assertRaises(sa.exc.DBAPIError):self.conn.exec_driver_sql(sql)
                transaction.rollback()

    def test_account_isolation_invalid_input_and_stale_correction(self):
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.second.id}/debtor/{self.client_account.id}/activity').status_code,404)
        self.assertEqual(self.client.post(self.url,data=dict(entry_key=uuid4().hex,work_date='bad',description='Work')).status_code,400)
        key=uuid4().hex
        self.client.post(self.url,data=dict(entry_key=key,work_date='2026-11-14',description='Original'))
        original=Activity.query.one()
        self.client.post(self.url,data=dict(previous_id=original.id,work_date='2026-11-14',description='Correction'))
        self.assertEqual(self.client.post(self.url,data=dict(previous_id=original.id,work_date='2026-11-14',description='Stale edit')).status_code,409)
        with self.client.session_transaction() as cookie:cookie['_user_id']='2'
        self.assertEqual(self.client.get(self.url).status_code,404)
        self.assertEqual(self.client.post(self.url,data=dict(entry_key=key,work_date='2026-11-14',description='Unauthorized')).status_code,404)
        self.assertEqual(Activity.query.count(),2)


if __name__=='__main__':unittest.main()
