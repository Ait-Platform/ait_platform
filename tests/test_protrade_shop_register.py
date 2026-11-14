"""Stage 1 regressions: real route bodies, session-temporary PostgreSQL tables.
Run directly: .venv/Scripts/python.exe tests/test_protrade_shop_register.py
No application factory/startup migrations or persistent test writes.
"""
import ast
import importlib.util
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from dotenv import dotenv_values
from flask import Flask, Blueprint, request, session, flash, redirect, url_for, render_template, current_app
from flask_login import LoginManager, login_required, current_user
from jinja2 import ChoiceLoader, DictLoader
from lxml import html

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('shop_test_models', ROOT / 'tests/test_protrade_dashboard.py')
models = importlib.util.module_from_spec(spec)
spec.loader.exec_module(models)
models.MechShop.bank_details = sa.Column(sa.Text)
for field in ('logo_url', 'letterhead_url'):
    setattr(models.SenderProfile, field, sa.Column(sa.String(255)))
models.SenderProfile.use_custom_letterhead = sa.Column(sa.Boolean, default=False)

# Production debtor table identity, isolated from the older dashboard fixture.
class ShopDebtor(models.Base):
    __table__ = models.Debtor.__table__.to_metadata(models.Base.metadata, name='debtor')
models.Debtor = ShopDebtor
models.Debtor.shop_id = sa.Column(sa.Integer, sa.ForeignKey('mech_shops.id'))
models.Debtor.address = sa.Column(sa.Text)
models.Debtor.opening_balance = sa.Column(sa.Integer,default=0)
models.Debtor.bank_account_id = sa.Column(sa.Integer)
models.MechClient.shop_id = sa.Column(sa.Integer, sa.ForeignKey('mech_shops.id'))

# Load the actual price-item mapping without initializing the application.
from datetime import datetime
catalog_node = next(n for n in ast.parse((ROOT / 'app/models/mechanic.py').read_text()).body
                    if isinstance(n, ast.ClassDef) and n.name == 'MechCatalogPart')
catalog_db = SimpleNamespace(Model=models.Base, Column=sa.Column, Integer=sa.Integer,
                             String=sa.String, Float=sa.Float, FetchedValue=sa.FetchedValue, DateTime=sa.DateTime, JSON=sa.JSON, Boolean=sa.Boolean, Date=sa.Date,
                             Text=sa.Text, Numeric=sa.Numeric, Computed=sa.Computed,
                             UniqueConstraint=sa.UniqueConstraint, CheckConstraint=sa.CheckConstraint, Index=sa.Index, text=sa.text,
                             ForeignKey=lambda target,**kwargs: sa.ForeignKey('users.id' if target == 'user.id' else target,**kwargs))
catalog_globals = dict(db=catalog_db, datetime=datetime)
exec(compile(ast.Module(body=[catalog_node], type_ignores=[]), 'production_catalog_model', 'exec'), catalog_globals)
MechCatalogPart = catalog_globals['MechCatalogPart']
boq_node = next(n for n in ast.parse((ROOT / 'app/models/mechanic.py').read_text()).body
                if isinstance(n, ast.ClassDef) and n.name == 'ProTradeBoqItem')
exec(compile(ast.Module(body=[boq_node], type_ignores=[]), 'production_boq_model', 'exec'), catalog_globals)
ProTradeBoqItem = catalog_globals['ProTradeBoqItem']
quote_node = next(n for n in ast.parse((ROOT / 'app/models/mechanic.py').read_text()).body
                  if isinstance(n, ast.ClassDef) and n.name == 'ProTradeBoqQuote')
exec(compile(ast.Module(body=[quote_node], type_ignores=[]), 'production_boq_quote_model', 'exec'), catalog_globals)
ProTradeBoqQuote = catalog_globals['ProTradeBoqQuote']
output_email_node=next(node for node in ast.parse((ROOT/'app/models/mechanic.py').read_text()).body if isinstance(node,ast.ClassDef) and node.name=='ProTradeOutputEmail')
exec(compile(ast.Module(body=[output_email_node],type_ignores=[]),'production_output_email', 'exec'),catalog_globals)
ProTradeOutputEmail=catalog_globals['ProTradeOutputEmail']
catalog_globals.update(TypeDecorator=sa.types.TypeDecorator,Numeric=sa.Numeric)
cents_node=next(node for node in ast.parse((ROOT/'app/models/debtors.py').read_text()).body if isinstance(node,ast.ClassDef) and node.name=='LedgerCents')
exec(compile(ast.Module(body=[cents_node],type_ignores=[]),'production_ledger_cents','exec'),catalog_globals)
ledger_node=next(node for node in ast.parse((ROOT/'app/models/debtors.py').read_text()).body if isinstance(node,ast.ClassDef) and node.name=='DebtorLedger')
exec(compile(ast.Module(body=[ledger_node],type_ignores=[]),'production_debtor_ledger','exec'),catalog_globals)
DebtorLedger=catalog_globals['DebtorLedger']

for class_name in ('MechLaborLine', 'MechPartLine', 'MechInvoice'):
    node = next(n for n in ast.parse((ROOT / 'app/models/mechanic.py').read_text()).body
                if isinstance(n, ast.ClassDef) and n.name == class_name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'production_document_model', 'exec'), catalog_globals)

models.MechJobCard.quotation_seq = sa.Column(sa.Integer, server_default=sa.FetchedValue())
for property_name in ('display_quote_number','display_document_number'):
    production=next(n for n in ast.parse((ROOT/'app/models/mechanic.py').read_text()).body if isinstance(n,ast.ClassDef) and n.name=='MechJobCard')
    method=next(n for n in production.body if isinstance(n,ast.FunctionDef) and n.name==property_name)
    namespace={}
    exec(compile(ast.Module(body=[method],type_ignores=[]),'production_quote_number_property','exec'),namespace)
    setattr(models.MechJobCard,property_name,namespace[property_name])
models.MechJobCard.shop_id = sa.Column(sa.Integer, sa.ForeignKey('mech_shops.id'))
models.MechJobCard.debtor_id = sa.Column(sa.Integer, sa.ForeignKey('debtor.id'))
models.MechJobCard.is_draft = sa.Column(sa.Boolean)
models.MechJobCard.labor_lines = sa.orm.relationship(catalog_globals['MechLaborLine'])
models.MechJobCard.part_lines = sa.orm.relationship(catalog_globals['MechPartLine'])
sa.Index('uq_protrade_mech_open_draft',models.MechJobCard.shop_id,models.MechJobCard.debtor_id,unique=True,postgresql_where=sa.text("status = 'Quote' AND is_draft IS TRUE AND debtor_id IS NOT NULL"))





class ProductionImports(ast.NodeTransformer):
    def visit_ImportFrom(self, node):
        return None if (node.module or '').startswith('app.models.') else node


class ShopRegisterTests(unittest.TestCase):
    def setUp(self):
        url = dotenv_values(ROOT / '.env')['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)
        parsed = sa.engine.make_url(url)
        self.assertEqual(parsed.database, 'ait_local_db')
        self.assertIn(parsed.host, ('localhost', '127.0.0.1', '::1'))
        self.engine = sa.create_engine(url)
        self.addCleanup(self.engine.dispose)
        self.conn = self.engine.connect()
        self.addCleanup(self.conn.close)
        self.tx = self.conn.begin()
        self.addCleanup(self.tx.rollback)
        self.assertEqual(self.conn.execute(sa.text('select current_database()')).scalar(), 'ait_local_db')
        self.conn.exec_driver_sql('CREATE TEMP TABLE stage1_sentinel (id integer)')
        self.conn.exec_driver_sql('SET LOCAL search_path TO pg_temp')
        models.Base.metadata.create_all(self.conn)
        self.db_session = scoped_session(sessionmaker(bind=self.conn, join_transaction_mode='create_savepoint'))
        self.addCleanup(self.db_session.remove)
        models.Base.query = self.db_session.query_property()
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        self.app.config.update(SECRET_KEY='stage1-test', TESTING=True)
        self.app.jinja_loader = ChoiceLoader([DictLoader({
            'layout.html': '{% block content %}{% endblock %}',
            'partials/flash_messages.html': '', 'program_mechanic/menu.html': ''
        }), self.app.jinja_loader])
        self.app.jinja_env.globals['csrf_token'] = lambda: ''
        manager = LoginManager(self.app)
        manager.user_loader(lambda uid: self.db_session.get(models.User, int(uid)))
        bp = Blueprint('mechanic_bp', __name__)
        tree = ast.parse((ROOT / 'app/program_mechanic/routes.py').read_text(encoding='utf-8'))
        names = {'ensure_sender_profile_synced', 'shop_register_rows', 'get_active_shop', 'mechanic_dashboard', 'onboarding_process',
                 'registered_trade_name', 'price_list_shop', 'price_list_query',
                 'selected_price_list', 'catalog_manage', 'carpentry_boq_price_list', 'client_register_quote', 'shop_register_quote',
                 'register_trade_quote', 'carpentry_quote_form', 'carpentry_quote_item_id', 'carpentry_quote_items', 'carpentry_quote_view', 'new_quote', 'quote_invoice_register', 'edit_quote',
                 'debtor_register_entry', 'shop_debtor_register', 'debtor_quote', 'new_debtor_quote','protrade_output','client_ledger','client_ledger_add_payment'}
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'AVAILABLE_TRADES' for t in n.targets)]
        catalogue = MechCatalogPart
        self.ns = dict(vars(models), db=SimpleNamespace(session=self.db_session, engine=self.conn),
                       mechanic_bp=bp, MechCatalogPart=catalogue, ProTradeBoqItem=ProTradeBoqItem, ProTradeBoqQuote=ProTradeBoqQuote, DebtorLedger=DebtorLedger, MechInvoice=catalog_globals['MechInvoice'],
                       MechLaborLine=catalog_globals['MechLaborLine'], MechPartLine=catalog_globals['MechPartLine'], request=request, session=session,
                       flash=flash, redirect=redirect, url_for=url_for, render_template=render_template,
                       current_app=current_app, current_user=current_user, login_required=login_required)
        code = ast.fix_missing_locations(ProductionImports().visit(ast.Module(body=nodes, type_ignores=[])))
        exec(compile(code, 'production_shop_routes', 'exec'), self.ns)
        self.app.register_blueprint(bp)
        template = (ROOT / 'templates/program_mechanic/dashboard.html').read_text(encoding='utf-8')
        template += (ROOT / 'templates/program_mechanic/menu.html').read_text(encoding='utf-8')
        template += (ROOT / 'templates/program_mechanic/quote_form.html').read_text(encoding='utf-8')
        template += (ROOT / 'templates/program_mechanic/quote_invoice_register.html').read_text(encoding='utf-8')
        template += (ROOT / 'templates/program_mechanic/shop_debtors.html').read_text(encoding='utf-8')
        endpoints = set(re.findall(r"url_for\('([^']+)'", template)) | {'mechanic_bp.view_client', 'mechanic_bp.job_card_detail', 'mechanic_bp.mock_bill'}
        for endpoint in endpoints:
            if endpoint not in self.app.view_functions:
                self.app.add_url_rule('/stub/' + endpoint + '/<int:client_id>' if endpoint.endswith(('view_client', 'update_client')) else '/stub/' + endpoint + '/<int:id>' if endpoint.endswith('job_card_detail') else '/stub/' + endpoint,
                                      endpoint, lambda **kw: 'stub')
        self.db_session.add_all([models.User(id=1, name='Owner', email='one@example.test'), models.User(id=2, name='Other', email='two@example.test')])
        self.db_session.commit()
        self.client = self.app.test_client()
        with self.client.session_transaction() as sess:
            sess['_user_id'] = '1'
            sess['_fresh'] = True

    def shop(self, owner=1, name='Happy Shop', trade='mechanic', status='draft_setup'):
        item = models.MechShop(user_id=owner, business_name=name, trade=trade, onboarding_status=status,
                               phone='0111234567', address='Existing address', terms_and_conditions='Existing terms')
        self.db_session.add(item)
        self.db_session.commit()
        return item

    def register(self, query=''):
        response = self.client.get('/mechanic/dashboard' + query)
        self.assertEqual(response.status_code, 200)
        return html.fromstring(response.data).get_element_by_id('client-table-body')

    def test_existing_happy_shop_visible_with_owner_isolation_and_no_copies(self):
        happy = self.shop()
        self.shop(owner=2, name='Other Owner Shop')
        self.shop(name='   ', trade='generic')
        self.db_session.add(models.MechClient(user_id=1, name='Existing Customer'))
        self.db_session.commit()
        for _ in range(2):
            table = self.register()
            self.assertEqual(len(table.xpath('.//a[text()="Happy Shop"]')), 1)
            self.assertNotIn('Existing Customer', table.text_content())
            self.assertNotIn('Other Owner Shop', table.text_content())
            self.assertEqual(models.MechShop.query.count(), 3)
            self.assertEqual(models.MechClient.query.count(), 1)
        self.assertEqual(happy.user_id, 1)
        self.assertEqual(happy.address, 'Existing address')
        self.assertEqual(happy.terms_and_conditions, 'Existing terms')

    def test_save_reload_repeat_and_rename_keep_one_shop_and_owner(self):
        data = dict(trade='mechanic', business_name='Happy Shop', address='Saved address', phone='0123456789')
        for _ in range(2):
            self.assertEqual(self.client.post('/mechanic/onboarding/process', data=data).status_code, 302)
            self.assertIn('Happy Shop', self.register().text_content())
        item = models.MechShop.query.one()
        identifier = item.id
        data.update(shop_id=str(identifier), business_name='Happy Shop Updated')
        self.client.post('/mechanic/onboarding/process', data=data)
        self.assertIn('Happy Shop Updated', self.register().text_content())
        self.assertEqual(models.MechShop.query.one().id, identifier)
        self.assertEqual(item.user_id, 1)
        self.assertEqual(models.MechClient.query.count(), 0)

    def test_each_trade_and_same_trade_existing_record_remain_addressable(self):
        first = self.shop(status='active')
        second = self.shop(name='Second Shop', status='active')
        self.shop(name='Panel Shop', trade='panel', status='active')
        table = self.register()
        link = table.xpath('.//a[text()="Second Shop"]')[0].get('href')
        response = self.client.get(link)
        tree = html.fromstring(response.data)
        self.assertIn(f'/shop/{second.id}/debtors', link)
        self.assertIn('Debtor Register', tree.text_content())
        self.client.post('/mechanic/onboarding/process', data=dict(shop_id=second.id, trade='mechanic', business_name='Second Updated'))
        self.assertEqual(first.business_name, 'Happy Shop')
        self.assertEqual(second.business_name, 'Second Updated')
        self.assertEqual(models.MechShop.query.count(), 3)

    def test_shop_rows_participate_in_counter_and_show_more(self):
        for index in range(6):
            self.shop(name=f'Shop {index}', trade='panel' if index else 'mechanic', status='active')
        response = self.client.get('/mechanic/dashboard')
        tree = html.fromstring(response.data)
        self.assertIn('6 clients', tree.get_element_by_id('client-counter').text_content())
        self.assertEqual(len(tree.xpath('//tr[contains(@class,"client-row-hidden")]')), 1)
        self.assertIn('Show More (5 of 6)', tree.get_element_by_id('client-toggle-btn').text_content())
        self.assertEqual(len(tree.xpath('//table[@id="client-register-table"]/tbody/tr/td[last()]/*[normalize-space()="Debtor Register"]')), 6)

    def test_foreign_owner_profile_cannot_be_saved(self):
        other = self.shop(owner=2, name='Other Owner Shop')
        response = self.client.post('/mechanic/onboarding/process', data=dict(shop_id=other.id, trade='mechanic', business_name='Stolen'))
        self.assertEqual(response.status_code, 404)
        self.assertEqual(other.business_name, 'Other Owner Shop')
        self.assertEqual(other.user_id, 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
