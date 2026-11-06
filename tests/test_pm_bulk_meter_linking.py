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
    def test_confirmed_links_follow_latest_save_and_property_scope(self):
        from app.program_billing.bulk_meter_linking import confirmed_meters
        def recognised():
            rows=self.session.execute(text('SELECT id,parent_meter_id FROM bil_meter WHERE property_id=26')).mappings().all()
            return {r['id']:r['parent_meter_id'] for r in confirmed_meters(26,rows)}
        self.assertEqual(self.link().status_code,302)
        self.assertEqual(recognised()[91],90)
        # Another property's audit evidence cannot override the saved confirmation.
        self.session.execute(text("INSERT INTO bil_utilities_capture_audit(property_id,kind,record_id,actor_id,after_data) VALUES(28,'bulk_link',90,8,CAST(:d AS json))"),dict(d='{"source":"bulk_meter_linking","children":[]}'))
        self.session.commit();self.conn.commit()
        self.assertEqual(recognised()[91],90)
        self.assertEqual(self.link(children=()).status_code,302)
        self.assertIsNone(recognised()[91])
        # A stale pointer is not a replacement for the latest explicit save.
        self.session.execute(text('UPDATE bil_meter SET parent_meter_id=90 WHERE id=91'))
        self.session.commit();self.conn.commit()
        self.assertIsNone(recognised()[91])

    def test_invalid_duplicate_and_circular_links_are_rejected(self):
        for parent,children,utility in [(90,(90,),'water'),(90,(94,),'water'),(90,(93,),'water'),(90,(91,91),'water'),(92,(93,),'water')]:
            self.assertEqual(self.link(parent,children,utility).status_code,400)
        self.assertEqual(self.link().status_code,302)
        self.assertEqual(self.link(parent=95).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=91')).scalar(),95)
        self.assertEqual(self.link(parent=91,children=(95,)).status_code,400)
        self.assertEqual(self.session.execute(text('SELECT parent_meter_id FROM bil_meter WHERE id=91')).scalar(),95)
    def test_permissions_csrf_and_navigation_cleanup(self):
        self.assertEqual(self.client.get(self.bulk_url).status_code,403)
        self.assertEqual(self.client.get(self.bulk_url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.post(self.bulk_url,data={},headers=self.headers).status_code,400)
        page=self.client.get(self.bulk_url,headers=self.headers);self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        self.assertTrue(tree.xpath('//h1[starts-with(text(),"Link Meters")]'))
        back=tree.xpath('//a[text()="Back to Utilities"]/@href')[0]
        self.assertIn('property_id=26',back);self.assertIn('billing_month=2026-10',back)
        self.assertEqual(len(tree.xpath('//select[@name="edit"]')),2)
        self.assertEqual(len(tree.xpath('//table')),2)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(BulkLinkTests(name) for name in BulkLinkTests.__dict__ if name.startswith('test_'))
