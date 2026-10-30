"""Existing PM property table -> scoped registered tenant -> existing ledger."""
import types,sys,unittest
from unittest.mock import patch
from flask import request,abort,render_template,redirect,url_for,flash,current_app
from sqlalchemy import text,select,Table,MetaData
from sqlalchemy.orm import registry
from sqlalchemy.exc import SQLAlchemyError
from jinja2 import ChoiceLoader,DictLoader
from lxml import html
import test_pm_soa_ledger as fixture
import test_pm_lease_lessee_persistence as loader


class LedgerNavigationTests(unittest.TestCase):
    path=fixture.PMTenantLedgerTests.path
    def setUp(self):
        fixture.PMTenantLedgerTests.setUp(self)
        mapper=registry()
        class Property:pass
        mapper.map_imperatively(Property,Table('bil_property',MetaData(),autoload_with=self.conn));self.conn.commit()
        owners=types.ModuleType('app.program_billing.owner_management')
        owners.owners_enabled=lambda:False;owners.owner_rows=lambda p:[]
        mocked=patch.dict(sys.modules,{'app.program_billing.owner_management':owners});mocked.start();self.addCleanup(mocked.stop)
        env=dict(select=select,db=self.db,BilProperty=Property,current_user=self.user,request=request,
            render_pm_template=render_template,abort=abort,redirect=redirect,url_for=url_for,
            flash=flash,current_app=current_app,SQLAlchemyError=SQLAlchemyError)
        loader.functions('app/program_billing/property_management.py',{'property_owner_page'},env)
        self.app._got_first_request=False
        def dashboard():
            if request.headers.get('X-Test-Owner')!='yes':abort(403)
            return env['property_owner_page']()
        self.app.view_functions['billing_bp.learner_dashboard']=dashboard
        for endpoint in ('billing_bp.property_owner','billing_bp.property_edit','billing_bp.property_address','bridge_bp.bridge'):
            if endpoint not in self.app.view_functions:
                self.app.add_url_rule('/navigation-test/'+endpoint+'/<int:property_id>' if endpoint.endswith(('property_edit','property_address')) else '/navigation-test/'+endpoint,endpoint=endpoint,view_func=lambda **kw:'Existing property action')
        self.app.jinja_loader=ChoiceLoader([DictLoader({'program_billing/owner_assignment_preview.html':''}),self.app.jinja_loader])
    def test_property_row_ledger_button_uses_property_id_without_radio_or_menu_entry(self):
        dashboard=self.client.get('/soa-test/learner_dashboard',headers=self.headers)
        self.assertEqual(dashboard.status_code,200,dashboard.text)
        tree=html.fromstring(dashboard.data)
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]/a[text()="Ledger"]'))
        self.assertEqual(tree.xpath('//nav/a[@aria-current="page"]/text()'),['Property & Owner'])
        self.assertNotIn('Unrelated property',dashboard.text)
        self.assertFalse(tree.xpath('//select[@id="soa-property"]|//input[@type="radio"]|//form[@id="property-ledger-form"]|//button[text()="Open Ledger"]'))
        rows=tree.xpath('//*[@id="my-properties-table"]/tbody/tr')
        self.assertEqual(len(rows),2)
        for row in rows:
            self.assertEqual(len(row.xpath('./td//a[text()="Ledger"]')),1)
            self.assertEqual(len(row.xpath('./td/a[contains(@href,"property_edit")]')),1)
        links=tree.xpath('//table/tbody/tr/td//a[text()="Ledger"]/@href')
        self.assertEqual(set(links),{self.url+'?property_id=26&workflow=ledger',self.url+'?property_id=27&workflow=ledger'})
        single=next(link for link in links if 'property_id=27' in link)
        response=self.client.get(single,headers=self.headers)
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(response.location,self.path(27,22))
        self.assertEqual(self.client.get(response.location,headers=self.headers).status_code,200)
    def test_multiple_tenants_require_explicit_choice_with_no_second_property_selection(self):
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all()
        brand=self.client.get('/manager/tenant_accounts/26/branding',headers=self.headers)
        back=html.fromstring(brand.data).xpath('//a[text()="Back to Ledger"]/@href')
        self.assertEqual(back,[self.url+'?property_id=26&workflow=ledger'])
        page=self.client.get(self.url+'?property_id=26&workflow=ledger',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//select[@name="property_id"]'))
        self.assertIn('Tenant Ledger',page.text);self.assertIn('Synthetic property',page.text)
        links=tree.xpath('//tbody/tr/td/a[starts-with(text(),"Open Ledger")]/@href')
        self.assertEqual(set(links),{self.path(26,11),self.path(26,44)})
        for link in links:
            ledger=self.client.get(link,headers=self.headers)
            self.assertEqual(ledger.status_code,200)
            self.assertEqual(html.fromstring(ledger.data).xpath('//*[@id="ledger-actions"]/*/text()'),['Charges','Payment','Fixed Charges','Opening Balance','Letterhead','SOA'])
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all(),before)
    def test_permissions_direct_urls_and_legacy_bookmarks_are_preserved(self):
        self.assertEqual(self.client.get(self.url+'?property_id=28&workflow=ledger',headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url+'?property_id=bad&workflow=ledger',headers=self.headers).status_code,400)
        self.assertEqual(self.client.get(self.url+'?property_id=26&workflow=ledger').status_code,403)
        self.assertEqual(self.client.get(self.path(26,22),headers=self.headers).status_code,404)
        for url in (self.url,self.url+'?property_id=26',self.path(26,11)):
            self.assertEqual(self.client.get(url,headers=self.headers).status_code,200)
        response=self.client.get(self.url+'?workflow=ledger',headers=self.headers)
        self.assertEqual(response.status_code,302);self.assertEqual(response.location,'/soa-test/learner_dashboard?workflow=ledger')
        self.user.id=8
        self.assertEqual(self.client.get(self.url+'?property_id=26&workflow=ledger',headers=self.headers).status_code,404)
    def test_property_without_tenants_never_fabricates_or_selects_another_tenant(self):
        self.session.execute(text("INSERT INTO bil_property VALUES(29,'Empty property','Empty address',7,false)"));self.session.commit()
        page=self.client.get(self.url+'?property_id=29&workflow=ledger',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertIn('No lease tenants found for this property.',page.text)
        self.assertFalse(html.fromstring(page.data).xpath('//tbody/tr/td/a[starts-with(text(),"Open Ledger")]'))

    def test_normal_property_management_links_unchanged_and_archived_ledger_disabled(self):
        self.session.execute(text("INSERT INTO bil_property VALUES(29,'Archived property','Old address',7,true)"));self.session.commit()
        normal=self.client.get('/soa-test/learner_dashboard',headers=self.headers)
        self.assertEqual(normal.status_code,200,normal.text)
        tree=html.fromstring(normal.data)
        self.assertTrue(tree.xpath('//table/tbody/tr/td[1]/a[contains(@href,"property_edit")]'))
        ledger=self.client.get('/soa-test/learner_dashboard?workflow=ledger',headers=self.headers)
        tree=html.fromstring(ledger.data)
        self.assertTrue(tree.xpath('//table/tbody/tr[td/a[text()="Archived property"]]//button[text()="Ledger"][@disabled]'))
        self.assertTrue(tree.xpath('//table/tbody/tr/td/a[text()="Archived property"]'))
        self.assertEqual(self.client.get(self.url+'?property_id=29&workflow=ledger',headers=self.headers).status_code,404)

    def test_property_ledger_action_requires_one_authorised_property_id(self):
        missing=self.client.get(self.url+'?workflow=ledger',headers=self.headers)
        self.assertEqual(missing.status_code,302)
        self.assertEqual(missing.location,'/soa-test/learner_dashboard?workflow=ledger')
        self.assertEqual(self.client.get(self.url+'?workflow=ledger&property_id=26&property_id=27',headers=self.headers).status_code,400)
        self.assertEqual(self.client.get(self.url+'?workflow=ledger&property_id=28',headers=self.headers).status_code,404)

    def test_owner_edit_assignment_and_address_controls_remain_independent(self):
        owners=sys.modules['app.program_billing.owner_management']
        owners.owners_enabled=lambda:True
        owners.owner_rows=lambda p:[dict(owner_id=501,name='Registered Owner')]
        self.app._got_first_request=False
        self.app.add_url_rule('/navigation-test/owner/<int:owner_id>',endpoint='billing_bp.owner_edit',view_func=lambda **kw:'Existing owner editing')
        page=self.client.get('/soa-test/learner_dashboard?workflow=ledger',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//button[@data-owner-edit]')),2)
        self.assertEqual(len(tree.xpath('//button[@data-open-owner]')),2)
        self.assertEqual(len(tree.xpath('//button[@data-address-dialog]')),2)
        self.assertTrue(all(button.get('type')=='button' for button in tree.xpath('//table//button')))
        self.assertFalse(tree.xpath('//script[contains(@src,"pm_ledger_selection.js")]'))
        self.assertFalse(tree.xpath('//form[@id="property-ledger-form"]//table'))

    def test_property_utilities_actions_and_wrapping(self):
        self.session.execute(text("UPDATE bil_property SET address=:address WHERE id=26"),dict(address='Long address '*12));self.session.commit()
        page=self.client.get('/soa-test/learner_dashboard',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="my-properties-table"]/thead/tr/th/text()'),['Property','Address','Owners','Actions'])
        rows=tree.xpath('//*[@id="my-properties-table"]/tbody/tr')
        self.assertEqual(len(rows),2)
        for row in rows:
            self.assertEqual(row.xpath('./td[4]//a/text()'),['Ledger','Utilities'])
            self.assertIn('whitespace-normal',row.xpath('./td[2]/@class')[0])
        self.assertEqual(set(tree.xpath('//*[@id="my-properties-table"]//a[text()="Utilities"]/@href')),{'/soa-test/utilities_hub?property_id=26','/soa-test/utilities_hub?property_id=27'})
