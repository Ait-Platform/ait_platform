"""First Part A save creates exactly one canonical JSON draft; existing IDs stay scoped."""
import copy,io,uuid,unittest
from urllib.parse import urlparse,parse_qs
from zipfile import ZipFile
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError
from lxml import etree,html
import test_pm_lease_multiple_tenants as multi

class FirstPartASaveTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=multi.MultipleTenantsTests.setUp
    sign_in=multi.MultipleTenantsTests.sign_in
    sql=multi.MultipleTenantsTests.sql
    payload=multi.MultipleTenantsTests.payload
    assign=multi.MultipleTenantsTests.assign
    create=multi.MultipleTenantsTests.create
    stored=multi.MultipleTenantsTests.stored
    route=multi.MultipleTenantsTests.route
    post=multi.MultipleTenantsTests.post
    generate=multi.MultipleTenantsTests.generate
    owners=multi.MultipleTenantsTests.owners
    schema=multi.MultipleTenantsTests.schema
    form=multi.MultipleTenantsTests.form
    register=multi.MultipleTenantsTests.register
    save_party=multi.MultipleTenantsTests.save_party
    prepare=multi.MultipleTenantsTests.prepare
    complete_other_parts=multi.MultipleTenantsTests.complete_other_parts

    def fresh(self):
        owners,ids=self.prepare()
        self.sql('DELETE FROM bil_lease_draft');self.session.commit()
        response=self.client.get('/billing/leases/property/26/particulars/parties')
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertEqual(tree.xpath('//form/fieldset[@disabled]'),[])
        token=tree.xpath('//input[@name="registration_token"]/@value')[0]
        uuid.UUID(token)
        self.assertIn('first successful Part A save',response.get_data(as_text=True))
        return owners,ids,token

    def data(self,owners,ids,token):
        return dict(parties_form_version='5',representative_owner_id=str(owners[0]['owner_id']),tenant_ids=[str(i) for i in ids],registration_token=token)

    def save_first(self,data,query=''):
        return self.client.post('/billing/leases/property/26/particulars/parties'+query,data=data)

    def test_first_save_replay_reopen_and_word_use_saved_json(self):
        owners,ids,token=self.fresh();data=self.data(owners,ids,token)
        result=self.save_first(data);self.assertEqual(result.status_code,302)
        draft_id=parse_qs(urlparse(result.location).query)['draft_id'][0]
        self.draft=uuid.UUID(draft_id)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        self.assertEqual(self.sql('SELECT version FROM bil_lease_draft').scalar(),1)
        saved=copy.deepcopy(self.stored())
        self.assertEqual([p['tenant_id'] for p in saved['_parties']['lessees']],ids)
        company=saved['_parties']['lessees'][2]
        self.assertEqual(company['signatory_name'],'Company Representative')
        self.assertEqual(company['representative_email'],'rep@example.com')
        replay=self.save_first(data);self.assertEqual(replay.status_code,302)
        self.assertEqual(result.location,replay.location);self.assertEqual(self.stored(),saved)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        self.assertEqual(self.sql('SELECT version FROM bil_lease_draft').scalar(),1)
        self.assertIn(draft_id,self.client.get(result.location).get_data(as_text=True))
        reopened=self.client.get(self.route('particulars/parties'))
        checked=html.fromstring(reopened.get_data(as_text=True)).xpath('//input[@name="tenant_ids" and @checked]/@value')
        self.assertEqual(set(checked),set(map(str,ids)))
        self.assertIn('Company Representative',reopened.get_data(as_text=True))
        self.complete_other_parts()
        # Registry changes cannot silently change the saved contracting identity.
        self.sql("UPDATE bil_tenant SET name='Changed registry name' WHERE identity_number='COMPANY-REG'")
        self.sql("UPDATE bil_tenant_identity SET legal_name='Changed registry representative'");self.session.commit()
        generated=self.generate();self.assertEqual(generated.status_code,200)
        with ZipFile(io.BytesIO(generated.data)) as archive:
            visible=''.join(etree.fromstring(archive.read('word/document.xml')).itertext())
            self.assertIn('Contracting Company',visible);self.assertIn('Company Representative',visible)
            self.assertNotIn('Changed registry name',visible);self.assertNotIn('Changed registry representative',visible)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)

    def test_invalid_or_unauthorized_supplied_ids_never_create_replacements(self):
        owners,ids,token=self.fresh();data=self.data(owners,ids,token)
        for supplied in ('not-a-uuid',str(uuid.uuid4())):
            self.assertEqual(self.save_first(dict(data,draft_id=supplied)).status_code,404)
            self.assertEqual(self.save_first(data,'?draft_id='+supplied).status_code,404)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),0)
        self.sign_in(8)
        other=self.client.post('/billing/leases/property/27/drafts',data={'registration_token':str(uuid.uuid4())})
        other_id=parse_qs(urlparse(other.location).query)['draft_id'][0]
        self.sign_in(7)
        self.assertEqual(self.save_first(dict(data,draft_id=other_id)).status_code,404)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft WHERE manager_id=7').scalar(),0)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)

    def test_invalid_first_save_and_commit_failure_leave_no_draft(self):
        owners,ids,token=self.fresh();data=self.data(owners,ids,token)
        self.assertEqual(self.save_first(dict(data,tenant_ids=[])).status_code,400)
        self.assertEqual(self.save_first(dict(data,registration_token='invalid')).status_code,400)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),0)
        # The one transaction includes the insert and all Part A JSON.
        with patch.object(self.session,'commit',side_effect=SQLAlchemyError('synthetic commit failure')):
            self.assertEqual(self.save_first(data).status_code,503)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),0)

    def test_existing_draft_version_updates_create_lease_and_token_conflicts(self):
        owners,ids,token=self.fresh();data=self.data(owners,ids,token)
        result=self.save_first(data);self.assertEqual(result.status_code,302)
        self.draft=uuid.UUID(parse_qs(urlparse(result.location).query)['draft_id'][0])
        self.assertEqual(self.save_first(dict(data,tenant_ids=[str(ids[0])])).status_code,409)
        self.assertEqual(len(self.stored()['_parties']['lessees']),3)
        changed=dict(data,draft_id=str(self.draft),version='1',tenant_ids=[str(ids[0])])
        self.assertEqual(self.save_first(changed).status_code,302)
        self.assertEqual(self.sql('SELECT version FROM bil_lease_draft').scalar(),2)
        self.assertEqual(len(self.stored()['_parties']['lessees']),1)
        self.assertEqual(self.save_first(changed).status_code,409)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        # Existing explicit Create Lease endpoint remains idempotent.
        new_token=str(uuid.uuid4())
        first=self.client.post('/billing/leases/property/26/drafts',data={'registration_token':new_token})
        second=self.client.post('/billing/leases/property/26/drafts',data={'registration_token':new_token})
        self.assertEqual(first.status_code,302);self.assertEqual(first.location,second.location)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),2)
        self.sql("INSERT INTO bil_property(id,name,address,manager_id,is_archived) VALUES (10028,'Another managed property','Synthetic',7,false)");self.session.commit()
        association=self.client.post('/billing/property-owner/10028/owners',json={'owner_id':owners[0]['owner_id']})
        self.assertEqual(association.status_code,200)
        conflict=self.client.post('/billing/leases/property/10028/particulars/parties',data=data)
        self.assertEqual(conflict.status_code,409)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),2)
