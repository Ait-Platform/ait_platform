"""Fixed-charge table layout, utilities definition and schema compatibility."""
import importlib.util
from pathlib import Path
import unittest
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_soa_fixed as fixture


def apply_utilities(connection):
    path=Path(__file__).resolve().parents[1]/'migrations/versions/pm_soa_004_fixed_utilities.py'
    spec=importlib.util.spec_from_file_location('pm_utilities_migration',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with patch.object(module,'op',Operations(MigrationContext.configure(connection))):module.upgrade()


class PMUtilityDefinitionTests(unittest.TestCase):
    path=fixture.PMFixedRentTests.path
    fixed_form=fixture.PMFixedRentTests.fixed_form
    save=fixture.PMFixedRentTests.save
    schedule=fixture.PMFixedRentTests.schedule
    run_due=fixture.PMFixedRentTests.run_due
    def setUp(self):
        fixture.PMFixedRentTests.setUp(self)
        apply_utilities(self.conn);self.conn.commit()
    def utility_form(self,charge_type='water',**changes):
        page=self.client.get(self.path()+'?fixed=1&fixed_add=utility',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        data={e.get('name'):e.get('value','') for e in tree.xpath('//form//input') if e.get('name')}
        data.update(charge_type=charge_type,amount='500.25',start_date='2026-10-01',
            end_date='2027-09-30',day_of_month='1',status='active',fixed_agreed='1')
        data.update(changes);return data
    def test_two_tables_and_independent_add_buttons_with_rent_preserved(self):
        self.schedule(start_date='2026-10-01')
        utility=self.utility_form()
        self.assertEqual(self.save(utility).status_code,302)
        self.assertEqual(self.save(utility).status_code,302)
        self.assertEqual(self.save(dict(utility,charge_type='electricity')).status_code,409)
        page=self.client.get(self.path()+'?fixed=1',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="monthly-rent"]/thead/tr/th/text()'),
            ['Description','Monthly amount','Due day','Start date','End date','Status','Edit'])
        self.assertEqual(tree.xpath('//*[@id="utility-charges"]/thead/tr/th/text()'),
            ['Description','Amount','Due day','Start date','End date','Status','Edit'])
        self.assertEqual(len(tree.xpath('//section//a[text()="Add Charge"]|//section//button[text()="Add Charge"]')),2)
        self.assertIn('Rent Due',tree.xpath('string(//*[@id="monthly-rent"])'))
        self.assertIn('Water',tree.xpath('string(//*[@id="utility-charges"])'))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
        self.assertFalse(tree.xpath('//form'))
    def test_agreement_is_required_and_only_real_utility_categories_accepted(self):
        for changes in ({'fixed_agreed':''},{'charge_type':'variable_water'},{'amount':'0'},{'amount':'NaN'}):
            self.assertEqual(self.save(self.utility_form(**changes)).status_code,400)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_recurring WHERE pm_rent_schedule')).scalar(),0)
        self.assertEqual(self.save(self.utility_form()).status_code,302)
        row=self.session.execute(text('SELECT * FROM bil_tenant_recurring WHERE pm_rent_schedule')).mappings().one()
        self.assertEqual(row['description'],'Water');self.assertTrue(row['pm_fixed_agreed'])
        self.assertEqual(row['is_active'],0)
    def test_utility_edit_is_versioned_scoped_and_preserves_rent(self):
        self.schedule(start_date='2026-10-01')
        self.assertEqual(self.save(self.utility_form()).status_code,302)
        page=self.client.get(self.path()+'?fixed=1',headers=self.headers)
        link=html.fromstring(page.data).xpath('//*[@id="utility-charges"]//a[text()="Edit"]/@href')[0]
        editor=self.client.get(link,headers=self.headers);tree=html.fromstring(editor.data)
        data={e.get('name'):e.get('value','') for e in tree.xpath('//form//input') if e.get('name')}
        data.update(status='inactive',amount='700.50',fixed_agreed='1')
        self.assertEqual(self.save(data).status_code,302)
        self.assertEqual(self.save(data).status_code,302)
        rows=self.session.execute(text('SELECT * FROM bil_tenant_recurring WHERE pm_rent_schedule')).mappings().all()
        water=next(r for r in rows if r['pm_charge_type']=='water')
        rent=next(r for r in rows if r['description']=='Rent Due')
        self.assertEqual(water['amount'],Decimal('700.50'));self.assertFalse(water['pm_active'])
        self.assertEqual(rent['amount'],Decimal('35000.00'))
        self.assertEqual(self.save(dict(data,charge_type='electricity')).status_code,400)
        self.assertEqual(self.client.get(self.path(27,22)+'?fixed=1&fixed_edit='+str(water['id']),headers=self.headers).status_code,404)
    def test_stage_two_worker_preserves_rent_and_does_not_yet_post_utilities(self):
        self.schedule(start_date='2026-10-01')
        self.assertEqual(self.save(self.utility_form()).status_code,302)
        self.assertEqual(self.run_due(),dict(posted=1,failed=0))
        self.assertEqual(self.session.execute(text('SELECT description FROM bil_tenant_ledger')).scalars().all(),['Rent Due'])
    def test_database_disallows_unagreed_utility_schedule(self):
        data=self.utility_form()
        self.assertEqual(self.save(data).status_code,302)
        self.session.commit()
        with self.assertRaises(IntegrityError):
            self.session.execute(text("UPDATE bil_tenant_recurring SET pm_fixed_agreed=false WHERE pm_charge_type='water'"))
        self.session.rollback()
        self.assertTrue(self.session.execute(text("SELECT pm_fixed_agreed FROM bil_tenant_recurring WHERE pm_charge_type='water'")).scalar())
