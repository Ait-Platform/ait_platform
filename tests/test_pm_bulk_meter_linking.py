"""Property-only bulk relationships on temporary PostgreSQL tables."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture

class BulkLinkTests(fixture.UtilitiesCaptureTests):
    bulk_url='/billing/utilities/26/bulk-meter-linking?billing_month=2026-10'
    def setUp(self):
        super().setUp()
        from app.program_billing import bulk_meter_linking
        mock=patch.object(bulk_meter_linking,'db',self.db);mock.start();self.addCleanup(mock.stop)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(90,26,'BULK-W','Water'),(91,26,'SUB-W','Water'),(92,26,'BULK-E','Electrical'),(93,26,'SUB-E','Electricity'),(94,28,'FOREIGN','Water'),(95,26,'OTHER-W','Water')"))
        self.session.commit();self.conn.commit()
    def link(self,parent=90,children=(91,),utility='water'):
        return self.capture_post(self.bulk_url,parent_id=str(parent),children=[str(c) for c in children],utility=utility)
    def test_water_and_electricity_save_edit_preserve_records(self):
        before=self.snapshot()
        self.assertEqual(self.link().status_code,302)
        self.assertEqual(self.link(parent=92,children=(93,),utility='electricity').status_code,302)
        self.assertEqual(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=91')).scalar(),90)
        self.assertEqual(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=93')).scalar(),92)
        self.assertEqual(before['bil_meter_reading'],self.snapshot()['bil_meter_reading'])
        self.assertEqual(before['bil_consumption'],self.snapshot()['bil_consumption'])
        self.assertEqual(before['bil_muni_account'],self.snapshot()['bil_muni_account'])
        page=self.client.get(self.bulk_url+'&edit=90',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);self.assertEqual(tree.xpath('//input[@name="children"][@checked]/@value'),['91'])
        self.assertEqual(self.link(children=()).status_code,302)
        self.assertIsNone(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=91')).scalar())
    def test_invalid_duplicate_and_circular_links_are_rejected(self):
        for parent,children,utility in [(90,(90,),'water'),(90,(94,),'water'),(90,(93,),'water'),(90,(91,91),'water'),(92,(93,),'water')]:
            self.assertEqual(self.link(parent,children,utility).status_code,400)
        self.assertEqual(self.link().status_code,302)
        self.assertEqual(self.link(parent=95).status_code,409)
        self.assertEqual(self.link(parent=91,children=(90,)).status_code,400)
        self.assertEqual(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=91')).scalar(),90)
    def test_permissions_csrf_and_menu_order(self):
        self.assertEqual(self.client.get(self.bulk_url).status_code,403)
        self.assertEqual(self.client.get(self.bulk_url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.post(self.bulk_url,data={},headers=self.headers).status_code,400)
        page=self.client.get(self.bulk_url,headers=self.headers);self.assertEqual(page.status_code,200)
        nav=html.fromstring(page.data).xpath('//nav[@aria-label="Municipal Utilities"]')[0]
        self.assertEqual(nav.xpath('./*[last()]/text()'),['Bulk Meter Linking'])

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(BulkLinkTests(name) for name in BulkLinkTests.__dict__ if name.startswith('test_'))
