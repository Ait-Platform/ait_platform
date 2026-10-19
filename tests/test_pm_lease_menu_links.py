"""One focused check of real Lease menu destinations and prerequisite links."""
import unittest
from lxml import html
from flask import render_template
import test_pm_lease_property_payments as fixture

class LeaseMenuLinksTests(unittest.TestCase):
    def test_menu_navigation_with_and_without_saved_context(self):
        f=fixture.PropertyPaymentsTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.migrate()
        before=dict(f.sql('SELECT * FROM bil_lease_draft').mappings().one())
        response=f.client.get('/billing/leases?property_id=26&draft_id='+str(f.draft))
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        links=tree.xpath('//nav[@aria-label="Lease submenu"]/a')
        self.assertEqual(len(links),8)
        self.assertFalse(tree.xpath('//nav[@aria-label="Lease submenu"]//*[@aria-disabled or @disabled or @onclick]'))
        from urllib.parse import urlsplit,parse_qs
        for link in links:
            href=link.get('href');url=urlsplit(href)
            self.assertIn('/property/26/',url.path)
            self.assertEqual(parse_qs(url.query)['draft_id'],[str(f.draft)])
            with f.app.test_request_context(href):
                endpoint,args=f.app.url_map.bind('localhost').match(url.path)
                self.assertTrue(endpoint.startswith('billing_bp.lease_'))
        for link in links[:5]+links[6:]:
            self.assertEqual(f.client.get(link.get('href')).status_code,200,link.get('href'))
        with f.app.test_request_context('/billing/leases'):
            for prop in (None,{'id':26}):
                rendered=html.fromstring(render_template('program_billing/lease_submenu.html',property=prop,lease_draft=None))
                fallback=rendered.xpath('//nav/a')
                self.assertEqual(len(fallback),8)
                for link in fallback:
                    self.assertTrue(link.get('href'))
                    self.assertNotEqual(link.get('href'),'#')
                if prop:
                    self.assertIn('/particulars/parties',fallback[5].get('href'))
                else:
                    self.assertTrue(all(a.get('href').endswith('#select-property') for a in fallback))
        self.assertEqual(dict(f.sql('SELECT * FROM bil_lease_draft').mappings().one()),before)
        f.sign_in(8)
        for link in links:
            self.assertEqual(f.client.get(link.get('href')).status_code,404)
