"""Single focused Premises open/save smoke check on disposable PostgreSQL."""
import copy,json,unittest
from lxml import html
from sqlalchemy import text
import test_pm_lease_history as fixture
import test_pm_lease_tenant_page_selection as lease

class SimplifiedPremisesSmokeTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.LeaseHistoryTests.setUp
    sign_in=fixture.LeaseHistoryTests.sign_in
    sql=fixture.LeaseHistoryTests.sql
    payload=fixture.LeaseHistoryTests.payload
    assign=fixture.LeaseHistoryTests.assign
    create=fixture.LeaseHistoryTests.create
    route=lease.TenantPageSelectionTests.route
    post=lease.TenantPageSelectionTests.post
    stored=lease.TenantPageSelectionTests.stored

    def test_form_opens_saves_without_removed_fields_and_preserves_history(self):
        hidden={'lease_scope':'part_only','property_legal_description':'Saved title','property_extent_sqm':'1200','leased_area_sqm':'300','unit_count':'2','leased_portion_description':'Saved portion','excluded_areas':'Saved exclusions','plan_a_identifier':'Saved A','plan_b_identifier':'Saved B'}
        items={'_premises':dict(hidden,property_name='Almond',property_address='Saved address'),'_parties':{'saved':'unchanged'},'s1-12':{'value':'Saved included portions'},'s1-13':{'value':'Saved extent'},'s1-14':{'value':'Saved plans'},'s1-15':{'value':'Saved parking','not_applicable':True,'reason':'Saved reason'},'s1-16':{'value':'Saved use'},'sc-01':{'value':'Saved conditions'},'_period':{'saved':'unchanged'},'_amounts':{'saved':'unchanged'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(items)});self.session.commit()
        url=self.route('particulars/premises')
        response=self.client.get(url);self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        form=tree.xpath('//form[@method="post"]')[0]
        visible=form.xpath('.//input[not(@type="hidden")]/@name | .//textarea/@name | .//select/@name')
        self.assertEqual(visible,['premises_property_name','premises_property_address'])
        self.assertNotIn('Plan A:',response.get_data(as_text=True));self.assertNotIn('Previously recorded unstructured',response.get_data(as_text=True))
        self.assertEqual(self.post('premises',{'premises_form_version':'2','premises_property_name':'Almond updated','premises_property_address':'New saved address'}).status_code,302)
        saved=self.stored()
        for key,value in hidden.items():self.assertEqual(saved['_premises'][key],value)
        for key,value in items.items():
            if key!='_premises':self.assertEqual(saved[key],value)
        self.assertEqual(saved['_premises']['property_name'],'Almond updated')
        self.assertIn('New saved address',self.client.get(url).get_data(as_text=True))
        from app.program_billing import lease_workflow as workflow
        values=workflow.premises_placeholder_values(saved)
        self.assertEqual(values['[LEASED PREMISES AREA]'],'300');self.assertEqual(values['[PLAN A IDENTIFIER]'],'Saved A')
        # A new snapshot also saves with no removed values supplied or invented.
        self.session.execute(text("UPDATE bil_lease_draft SET items='{}'::jsonb"));self.session.commit()
        self.assertEqual(self.post('premises',{'premises_form_version':'2','premises_property_name':'Almond','premises_property_address':'Address only'}).status_code,302)
        self.assertNotIn('leased_area_sqm',self.stored()['_premises'])
