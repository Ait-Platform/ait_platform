"""Part A multiple tenants use JSON snapshots; no new persistence structures."""
import copy,io,unittest
from zipfile import ZipFile
from lxml import etree
import test_pm_parties_tenant_register as fixture
from test_pm_lease_parties_simplified import Tags

class MultipleTenantsTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.TenantRegisterTests.setUp
    sign_in=fixture.TenantRegisterTests.sign_in
    sql=fixture.TenantRegisterTests.sql
    payload=fixture.TenantRegisterTests.payload
    assign=fixture.TenantRegisterTests.assign
    create=fixture.TenantRegisterTests.create
    stored=fixture.TenantRegisterTests.stored
    route=fixture.TenantRegisterTests.route
    post=fixture.TenantRegisterTests.post
    generate=fixture.TenantRegisterTests.generate
    owners=fixture.TenantRegisterTests.owners
    schema=fixture.TenantRegisterTests.schema
    form=fixture.TenantRegisterTests.form
    register=fixture.TenantRegisterTests.register
    save_party=fixture.TenantRegisterTests.save_party

    def prepare(self):
        self.schema();owners=self.owners()
        for values in [dict(legal_name='First Natural Tenant',identity_number='FIRST-ID'),dict(legal_name='Second Natural Tenant',identity_number='SECOND-ID'),dict(party_type='juristic_person',legal_name='Contracting Company',identity_number='COMPANY-REG',representative_name='Company Representative',representative_identity_number='REP-ID',representative_phone='0100000099',representative_email='rep@example.com')]:
            self.assertEqual(self.register(**values).status_code,302)
        return owners,[r[0] for r in self.sql('SELECT id FROM bil_tenant ORDER BY id')]

    def save_multiple(self,owner,ids):
        return self.post('parties',dict(parties_form_version='5',representative_owner_id=str(owner['owner_id']),tenant_ids=[str(i) for i in ids]))

    def complete_other_parts(self):
        import test_pm_lease_premises_offline as premises
        import test_pm_lease_period_offline as period
        import test_pm_lease_amounts_offline as amounts
        import test_pm_lease_part5_offline as banking
        for group,data in [('premises',premises.PremisesOfflineTests.form(self)),('period',period.PeriodOfflineTests.form(self)),('amounts',amounts.AmountsOfflineTests.form(self)),('amounts',banking.Part5OfflineTests.form(self))]:
            self.assertEqual(self.post(group,data).status_code,302)

    def test_save_reopen_merge_and_remove_contracting_tenants(self):
        owners,ids=self.prepare()
        self.assertEqual(self.save_multiple(owners[0],ids).status_code,302)
        saved=copy.deepcopy(self.stored());self.assertEqual([p['tenant_id'] for p in saved['_parties']['lessees']],ids)
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        checked=[int(a['value']) for _,a in Tags(html).tags if a.get('name')=='tenant_ids' and 'checked' in a]
        self.assertEqual(set(checked),set(ids))
        self.complete_other_parts();self.assertEqual(self.workflow_module.stage5_completion(self.stored()),[])
        before=copy.deepcopy(self.stored())
        result=self.generate();self.assertEqual(result.status_code,200)
        with ZipFile(io.BytesIO(result.data)) as archive:
            visible=''.join(etree.fromstring(archive.read('word/document.xml')).itertext())
            for value in ('First Natural Tenant','Second Natural Tenant','Contracting Company','FIRST-ID','SECOND-ID','COMPANY-REG','Company Representative','REP-ID'):self.assertIn(value,visible)
            self.assertNotRegex(visible,r'\[[^\[\]]+\]')
        old_generations=copy.deepcopy(self.stored()['_generations'])
        self.assertEqual(self.save_multiple(owners[0],[ids[0],ids[2]]).status_code,302)
        after=self.stored()
        self.assertEqual([p['tenant_id'] for p in after['_parties']['lessees']],[ids[0],ids[2]])
        self.assertEqual(after['_generations'],old_generations)
        for key in ('_premises','_period','_amounts','_banking_utilities'):
            if key in before:self.assertEqual(after[key],before[key])
        mapping=self.workflow_module.party_placeholder_values(after)
        self.assertNotIn('Second Natural Tenant',mapping['[S1 LEGAL TENANT]'])
        self.assertEqual(self.save_multiple(owners[0],[ids[0]]).status_code,302)
        self.assertNotIn('Company Representative',str(self.workflow_module.party_placeholder_values(self.stored())))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant').scalar(),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),1)

    def test_legacy_single_selection_and_add_from_register(self):
        owners,ids=self.prepare();self.assertEqual(self.save_party(owners[0],ids[0]).status_code,302)
        saved=copy.deepcopy(self.stored());self.assertNotIn('lessees',saved['_parties'])
        html=self.client.get(self.route('particulars/parties')+'&tenant_id='+str(ids[2])).get_data(as_text=True)
        checked=[int(a['value']) for _,a in Tags(html).tags if a.get('name')=='tenant_ids' and 'checked' in a]
        self.assertEqual(set(checked),{ids[0],ids[2]})
        self.assertEqual(self.stored(),saved)
        self.assertEqual(self.save_multiple(owners[0],[ids[0],ids[2]]).status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant').scalar(),3)

    def test_authorization_empty_duplicate_and_invalid_tenants_do_not_save(self):
        owners,ids=self.prepare();before=copy.deepcopy(self.stored())
        for selected,status in (([],400),([ids[0],ids[0]],400),([ids[0],'0'+str(ids[0])],400),([ids[0],9999],404)):
            self.assertEqual(self.save_multiple(owners[0],selected).status_code,status);self.assertEqual(self.stored(),before)
        self.sql("INSERT INTO bil_tenant(name,manager_id,party_type,identity_number,address,rent_includes_metro,email_statements) VALUES ('PRIVATE TENANT',8,'natural_person','PRIVATE-ID','Private address',0,false)");self.session.commit()
        private=self.sql("SELECT id FROM bil_tenant WHERE manager_id=8").scalar()
        self.assertEqual(self.save_multiple(owners[0],[ids[0],private]).status_code,404)
        self.assertNotIn('PRIVATE TENANT',self.client.get(self.route('particulars/parties')).get_data(as_text=True))
        self.sign_in(8);self.assertEqual(self.save_multiple(owners[0],ids).status_code,404)

    def test_selected_table_company_representative_and_owner_checkbox_persist(self):
        from lxml import html as parser
        owners,ids=self.prepare()
        initial=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        tree=parser.fromstring(initial)
        self.assertEqual(tree.xpath('//tbody[@id="selected-tenants-body"]/tr'),[])
        self.assertEqual(len(tree.xpath('//table[@id="available-tenants"]//input[@name="tenant_ids"]')),3)
        self.assertLess(initial.index('Add Tenant / View Tenant List'),initial.index('id="available-tenants"'))
        self.assertIn('/billing/leases/property/26/tenants?draft_id=',initial)
        self.assertEqual(self.save_multiple(owners[1],[ids[2]]).status_code,302)
        reopened=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        tree=parser.fromstring(reopened)
        rows=tree.xpath('//tbody[@id="selected-tenants-body"]/tr')
        self.assertEqual(len(rows),1);self.assertEqual(rows[0].get('data-selected-tenant-id'),str(ids[2]))
        for detail in ('Contracting Company','COMPANY-REG','Company Representative','REP-ID','0100000099','rep@example.com'):self.assertIn(detail,rows[0].text_content())
        self.assertNotIn('First Natural Tenant',rows[0].text_content())
        selected=tree.xpath('//input[@name="representative_owner_id" and @checked]/@value')
        self.assertEqual(selected,[str(owners[1]['owner_id'])])
        self.assertEqual(self.stored()['_parties']['lessor']['owner_id'],owners[1]['owner_id'])
        # Clearing the representative is allowed in the UI but cannot commit an invalid lease.
        before=copy.deepcopy(self.stored())
        response=self.post('parties',dict(parties_form_version='5',tenant_ids=[str(ids[2])]))
        self.assertEqual(response.status_code,400);self.assertEqual(self.stored(),before)
        self.complete_other_parts()
        generated=self.generate();self.assertEqual(generated.status_code,200)
        with ZipFile(io.BytesIO(generated.data)) as archive:
            visible=''.join(etree.fromstring(archive.read('word/document.xml')).itertext())
            self.assertIn('Contracting Company',visible);self.assertIn('Company Representative',visible)
            self.assertNotIn('First Natural Tenant',visible);self.assertNotIn('Second Natural Tenant',visible)
        self.assertEqual(self.save_multiple(owners[0],[ids[0],ids[2]]).status_code,302)
        tree=parser.fromstring(self.client.get(self.route('particulars/parties')).get_data(as_text=True))
        self.assertEqual(len(tree.xpath('//tbody[@id="selected-tenants-body"]/tr')),2)
        self.assertEqual(tree.xpath('//input[@name="representative_owner_id" and @checked]/@value'),[str(owners[0]['owner_id'])])
