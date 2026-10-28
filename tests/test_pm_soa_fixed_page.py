"""Dedicated Fixed Charges navigation on isolated PostgreSQL."""
import unittest
from pathlib import Path
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
from lxml import html
from sqlalchemy import text
import test_pm_soa_utilities as fixture


class FixedChargesPageTests(unittest.TestCase):
    setUp=fixture.PMUtilityDefinitionTests.setUp
    path=fixture.PMUtilityDefinitionTests.path
    fixed_form=fixture.PMUtilityDefinitionTests.fixed_form
    save=fixture.PMUtilityDefinitionTests.save
    utility_form=fixture.PMUtilityDefinitionTests.utility_form

    def test_dedicated_navigation_and_back_context(self):
        ledger=self.client.get(self.path(),headers=self.headers)
        tree=html.fromstring(ledger.data)
        self.assertFalse(tree.xpath('//*[@id="monthly-rent" or @id="utility-charges"]|//form[input[@name="action"][@value="fixed"]]'))
        link=tree.xpath('//a[text()="Fixed Charges"]/@href')[0]
        self.assertEqual(link,self.path()+'/fixed')
        page=self.client.get(link,headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//h1/text()'),['Fixed Charges'])
        self.assertEqual(tree.xpath('//a[text()="Back to Tenant Ledger"]/@href'),[self.path()])
        links=tree.xpath('//a[text()="Rent" or text()="Utilities"]/@href')
        self.assertEqual(links,[self.path()+'/fixed/rent',self.path()+'/fixed/utilities'])
        for link in links:
            page=self.client.get(link,headers=self.headers)
            self.assertEqual(page.status_code,200,page.text)
            formtree=html.fromstring(page.data)
            self.assertFalse(formtree.xpath('//dialog|//a[@target="_blank"]'))
            self.assertEqual(formtree.xpath('//a[text()="Back to Fixed Charges"]/@href'),[self.path()+'/fixed'])
        old=self.client.get(self.path()+'?fixed=1&fixed_add=utility',headers=self.headers)
        self.assertEqual(old.status_code,302)
        self.assertEqual(old.location,self.path()+'/fixed?fixed_add=utility')

    def test_legacy_service_confirmation_and_validation_stay_inside_tile(self):
        root=Path(__file__).resolve().parents[1]
        parent=(root/'templates/layout.html').read_text(encoding='utf-8')
        start=parent.index('{% block flashes %}')
        end=parent.index('{% endblock %}',parent.index('{% endblock %}',start)+1)+len('{% endblock %}')
        self.app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':parent[start:end]+'{% block content %}{% endblock %}'}),FileSystemLoader(root/'templates')])
        for data in (self.fixed_form(),self.utility_form()):
            response=self.save(data)
            self.assertEqual(response.status_code,302,response.text)
            self.assertIn(response.location,[self.path()+'/fixed/rent',self.path()+'/fixed/utilities'])
            page=self.client.get(response.location,headers=self.headers)
            tree=html.fromstring(page.data)
            self.assertEqual(len(tree.xpath('//*[@id="pm-fixed-charges"]//*[@id="flash-toast-container"]')),1)
            self.assertIn('Fixed Charges saved.',page.text)
        invalid=self.fixed_form(amount='-10')
        response=self.save(invalid)
        self.assertEqual(response.status_code,400)
        tree=html.fromstring(response.data)
        self.assertTrue(tree.xpath('//*[@id="pm-fixed-charges"]//*[@role="alert"]'))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)

    def test_dedicated_page_keeps_authentication_property_and_tenant_permissions(self):
        self.assertEqual(self.client.get(self.path()+'/fixed').status_code,403)
        for prop,tenant in ((28,33),(27,11),(26,22)):
            page=self.client.get(self.path(prop,tenant)+'/fixed',headers=self.headers)
            self.assertEqual(page.status_code,404)
