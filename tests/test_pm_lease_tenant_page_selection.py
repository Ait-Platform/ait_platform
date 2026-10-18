"""Selections are written only by the existing tenant page; Part A displays snapshots."""
import copy,io,json,unittest,uuid
from urllib.parse import parse_qs,urlparse
from zipfile import ZipFile
from lxml import etree,html
import test_pm_lease_multiple_tenants as fixture

class TenantPageSelectionTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.MultipleTenantsTests.setUp
    sign_in=fixture.MultipleTenantsTests.sign_in
    sql=fixture.MultipleTenantsTests.sql
    payload=fixture.MultipleTenantsTests.payload
    assign=fixture.MultipleTenantsTests.assign
    create=fixture.MultipleTenantsTests.create
    stored=fixture.MultipleTenantsTests.stored
    route=fixture.MultipleTenantsTests.route
    post=fixture.MultipleTenantsTests.post
    generate=fixture.MultipleTenantsTests.generate
    owners=fixture.MultipleTenantsTests.owners
    schema=fixture.MultipleTenantsTests.schema
    form=fixture.MultipleTenantsTests.form
    register=fixture.MultipleTenantsTests.register
    prepare=fixture.MultipleTenantsTests.prepare
    complete_other_parts=fixture.MultipleTenantsTests.complete_other_parts

    def select_form(self,ids,draft=True):
        url=self.route('tenants') if draft else '/billing/leases/property/26/tenants'
        response=self.client.get(url);self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        form=tree.xpath('//form[input[@name="action" and @value="select_tenants"]]')[0]
        data={n.get('name'):n.get('value','') for n in form.xpath('.//input[@type="hidden"]')}
        data['tenant_ids']=[str(i) for i in ids]
        return url,data

    def select(self,ids,draft=True):
        url,data=self.select_form(ids,draft)
        return self.client.post(url,data=data)

    def save_parties(self,owner,**extra):
        return self.post('parties',dict(parties_form_version='6',representative_owner_id=str(owner['owner_id']),**extra))

    def selected_rows(self):
        response=self.client.get(self.route('particulars/parties'));self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertEqual(tree.xpath('//input[@name="tenant_ids"]'),[])
        self.assertNotIn('Available Tenants',response.get_data(as_text=True))
        self.assertEqual(tree.xpath('//button[@data-remove-tenant]'),[])
        return tree.xpath('//tbody[@id="selected-tenants-body"]/tr')

    def test_selection_page_return_part_a_save_reopen_and_word(self):
        owners,ids=self.prepare()
        self.assertEqual(self.select([ids[0],ids[2]]).status_code,302)
        saved=copy.deepcopy(self.stored())
        self.assertEqual([p['tenant_id'] for p in saved['_parties']['lessees']],[ids[0],ids[2]])
        rows=self.selected_rows();self.assertEqual(len(rows),2)
        for value in ('Company Representative','REP-ID','0100000099','rep@example.com'):self.assertIn(value,rows[1].text_content())
        self.assertNotIn('Second Natural Tenant',' '.join(row.text_content() for row in rows))
        self.assertEqual(self.save_parties(owners[0],tenant_ids=[str(ids[1])]).status_code,302)
        self.assertEqual(self.stored()['_parties']['lessees'],saved['_parties']['lessees'])
        self.complete_other_parts();before=copy.deepcopy(self.stored())
        self.sql("UPDATE bil_tenant_identity SET legal_name='Changed representative',email='changed@example.com'");self.session.commit()
        self.assertIn('Company Representative',self.selected_rows()[1].text_content())
        generated=self.generate();self.assertEqual(generated.status_code,200)
        with ZipFile(io.BytesIO(generated.data)) as archive:
            visible=''.join(etree.fromstring(archive.read('word/document.xml')).itertext())
            for value in ('First Natural Tenant','Contracting Company','Company Representative'):self.assertIn(value,visible)
            for value in ('Second Natural Tenant','Changed representative'):self.assertNotIn(value,visible)
        generations=copy.deepcopy(self.stored()['_generations'])
        self.assertEqual(self.select([ids[0]]).status_code,302)
        self.assertEqual(len(self.selected_rows()),1)
        self.assertEqual(self.stored()['_generations'],generations)
        for key in ('_premises','_period','_amounts','_banking_utilities'):
            if key in before:self.assertEqual(self.stored()[key],before[key])
        self.assertNotIn('Company Representative',str(self.workflow_module.party_placeholder_values(self.stored())))

    def test_selection_without_draft_creates_one_and_retains_id(self):
        owners,ids=self.prepare();self.sql('DELETE FROM bil_lease_draft');self.session.commit()
        url,data=self.select_form([ids[2]],draft=False)
        first=self.client.post(url,data=data);self.assertEqual(first.status_code,302)
        self.draft=uuid.UUID(parse_qs(urlparse(first.location).query)['draft_id'][0])
        repeat=self.client.post(url,data=data);self.assertEqual(repeat.status_code,302);self.assertEqual(first.location,repeat.location)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        self.assertEqual(len(self.selected_rows()),1)
        self.assertEqual(self.save_parties(owners[1]).status_code,302)
        self.assertEqual(self.stored()['_parties']['lessor']['owner_id'],owners[1]['owner_id'])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        # Adding a registry entry does not select it or overwrite saved draft JSON.
        before=copy.deepcopy(self.stored())
        created=self.register(legal_name='New unselected tenant',identity_number='NEW-ID')
        self.assertEqual(created.status_code,302);self.assertIn('/tenants?',created.location)
        self.assertEqual(self.stored(),before)
        self.assertNotIn('New unselected tenant',self.selected_rows()[0].text_content())

    def test_selection_authorization_version_and_part_a_no_alternate_writer(self):
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0]]).status_code,302)
        before=copy.deepcopy(self.stored())
        url,data=self.select_form([ids[2]])
        self.assertEqual(self.client.post(url,data=dict(data,version='0')).status_code,409)
        self.assertEqual(self.stored(),before)
        for choices in ([],[ids[0],ids[0]],[9999]):
            result=self.select(choices);self.assertIn(result.status_code,(400,404));self.assertEqual(self.stored(),before)
        self.assertEqual(self.post('parties',dict(parties_form_version='5',representative_owner_id=str(owners[0]['owner_id']),tenant_ids=[str(ids[2])])).status_code,400)
        self.assertEqual(self.stored(),before)
        self.sql("INSERT INTO bil_tenant(name,manager_id,party_type,identity_number,address,rent_includes_metro,email_statements) VALUES ('Private',8,'natural_person','PRIVATE','Private address',0,false)");self.session.commit()
        private=self.sql('SELECT id FROM bil_tenant WHERE manager_id=8').scalar()
        self.assertEqual(self.select([private]).status_code,404)
        self.sign_in(8);self.assertEqual(self.client.get(self.route('tenants')).status_code,404)

    def test_legacy_single_tenant_snapshot_remains_displayable(self):
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0]]).status_code,302)
        items=copy.deepcopy(self.stored());items['_parties'].pop('lessees')
        self.session.execute(__import__('sqlalchemy').text('UPDATE bil_lease_draft SET items=CAST(:j AS jsonb)'),{'j':json.dumps(items)});self.session.commit()
        self.assertEqual(len(self.selected_rows()),1)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        self.assertEqual(self.stored()['_parties']['lessees'][0]['tenant_id'],ids[0])
