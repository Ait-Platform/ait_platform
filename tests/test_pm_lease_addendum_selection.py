"""Stage 1 only: actual catalogs and persistent registers in isolated PostgreSQL."""
import copy,io,json,uuid,unittest
from lxml import html
from sqlalchemy import text
import test_pm_lease_addendum_pdf as fixture

class AddendumSelectionTests(unittest.TestCase):
    setUp=fixture.AddendumPDFTests.setUp
    stored=fixture.AddendumPDFTests.stored
    payload=fixture.AddendumPDFTests.payload
    sql=fixture.AddendumPDFTests.sql
    signed=fixture.AddendumPDFTests.signed
    pdf=fixture.AddendumPDFTests.pdf
    def register_url(self,draft=None):return '/billing/leases/property/26/addendum?draft_id='+str(draft or self.draft)
    def state(self,key=None):
        with self.app.test_request_context('/?draft_id='+str(self.draft),headers=self.headers):
            return self.history_env['clause_register_state'](self.history_env['locked'](26,self.draft),key)
    def choose(self,entry,checked=True,**changes):
        data={'csrf_token':self.csrf,'action':'addendum_clause_selection','clause_lease':self.state()['lease_key'],'register_version':str(self.stored()['version']),'provision_id':entry['id'],'selected':'yes' if checked else 'no'};data.update(changes)
        return self.client.post(self.register_url(),data=data)
    def test_selection_all_four_sources_persistence_removal_duplicates_and_exact_wording(self):
        state=self.state();before=copy.deepcopy(dict(self.stored()));chosen=[]
        for source in ('certificate','core','special_conditions','signature'):
            entry=next(entry for entry in state['catalog'] if entry['source']==source);chosen.append(entry)
            result=self.choose(entry,original='FORGED CLIENT WORDING');self.assertEqual(result.status_code,200,result.text)
        result=self.choose(chosen[0]);self.assertEqual(len(result.json['selected']),4)
        for original,saved in zip(chosen,result.json['selected']):self.assertEqual(saved['original'],original['original'])
        page=self.client.get(self.register_url());self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);saved=json.loads(tree.xpath('//*[@id="addendum-clause-data"]/text()')[0]);self.assertEqual(len(self.state()['selected']),4)
        self.assertFalse(tree.xpath('//button[@data-clause-source]'))
        self.assertNotIn('SELECTED CLAUSES REGISTER',page.text);self.assertNotIn('CLAUSE RELATIONSHIP REGISTER',page.text)
        result=self.choose(chosen[1],False);self.assertEqual(len(result.json['selected']),3);self.assertNotIn(chosen[1]['id'],[entry['id'] for entry in self.state()['selected']])
        after=self.stored()['items'];self.assertEqual({k:v for k,v in after.items() if k!='_addendum_clause_registers'},before['items'])
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_document')).scalar(),0)
    def test_selection_relationships_are_explicit_and_do_not_select_suggestions(self):
        items=copy.deepcopy(self.stored()['items']);clauses=items['_supplementary_conditions']['clauses']
        clauses[0]['text']='Unrelated rental heading alone is not a verified relationship.'
        clauses[1]['text']='This condition supplements Core Lease clause 5.1. Preserve this exact wording.'
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.draft,'items':json.dumps(items)});self.session.commit()
        state=self.state();core=next(e for e in state['catalog'] if e['source']=='core' and e['reference']=='5.1');sc=next(e for e in state['catalog'] if e['source']=='special_conditions' and e['reference']=='SC-02');unrelated=next(e for e in state['catalog'] if e['source']=='special_conditions' and e['reference']=='SC-01')
        self.assertTrue(any(e['left']==sc['id'] and e['right']==core['id'] and 'Explicit reference' in e['evidence'] for e in state['relationships']))
        self.assertFalse(any(e['left']==unrelated['id'] for e in state['relationships']))
        result=self.choose(core);self.assertEqual([e['id'] for e in result.json['selected']],[core['id']])
        self.assertTrue(any(e['right']==core['id'] for e in result.json['relationships']))
        certificate=next(e for e in state['catalog'] if e['source']=='certificate' and e['reference']=='Monthly rental')
        self.assertTrue(any({e['left'],e['right']}=={core['id'],certificate['id']} for e in state['relationships']))
        self.assertNotIn('For the owner',core['original']);self.assertIn('5.1 The Lessee',core['original'])
        self.assertEqual(len({e['id'] for e in state['catalog']}),len(state['catalog']))
    def test_selection_manual_pdf_exact_reference_no_stale_database_clauses(self):
        upload=self.client.post(self.endpoint,data={'csrf_token':self.csrf,'action':'sc_pdf_upload','source':(io.BytesIO(self.pdf()),'Conditions.pdf')},content_type='multipart/form-data')
        self.assertEqual(upload.status_code,200,upload.text)
        page=self.client.get(self.register_url());tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//*[@id="addendum-manual-sc"]'))
        state=self.state();self.assertFalse(any(e['source']=='special_conditions' for e in state['catalog']))
        wording='SC-A.7 Exact original wording.\n(a) Keep internal numbering and spaces.  '
        data={'csrf_token':self.csrf,'action':'addendum_manual_clause','clause_lease':'draft','register_version':str(self.stored()['version']),'reference':'SC-A.7','description':'Exact map provision','original':wording}
        result=self.client.post(self.register_url(),data=data);self.assertEqual(result.status_code,200,result.text);entry=result.json['selected'][0]
        self.assertEqual(entry['reference'],'SC-A.7');self.assertEqual(entry['original'],wording);self.assertTrue(entry['manual']);self.assertIn('not automatically verified',entry['provenance']);self.assertFalse(any(e['left']==entry['id'] for e in result.json['relationships']))
        data['register_version']=str(self.stored()['version']);self.assertEqual(self.client.post(self.register_url(),data=data).status_code,200);self.assertEqual(len(self.state()['selected']),1)
        data.update(register_version=str(self.stored()['version']),original='Changed original wording');self.assertEqual(self.client.post(self.register_url(),data=data).status_code,409);self.assertEqual(self.state()['selected'][0]['original'],wording)
        self.assertEqual(self.choose(entry,False).status_code,200);self.assertEqual(self.state()['selected'],[])
    def test_selection_snapshot_isolation_stale_versions_and_access_controls(self):
        self.signed();first=self.state()['lease_key'];state=self.state();entry=next(e for e in state['catalog'] if e['source']=='special_conditions');self.assertEqual(self.choose(entry).status_code,200)
        snapshot=copy.deepcopy(self.ledger_original(first))
        self.signed();second=self.state(self.ledger_latest())['lease_key'];self.assertNotEqual(first,second);self.assertEqual(self.state(second)['selected'],[])
        self.assertEqual(self.ledger_original(first),snapshot)
        before=copy.deepcopy(dict(self.stored()));self.assertEqual(self.choose(entry,register_version='0').status_code,409);self.assertEqual(dict(self.stored()),before)
        self.assertEqual(self.choose(entry,clause_lease=str(uuid.uuid4())).status_code,404)
        self.assertEqual(self.choose({'id':'foreign-provision'}).status_code,404)
        self.client.environ_base.pop('HTTP_X_TEST_OWNER');self.assertEqual(self.choose(entry).status_code,403)
    def test_selection_draft_scope_and_snapshot_source_do_not_follow_later_edits(self):
        self.signed();key=self.state()['lease_key'];original=self.state(key)
        items=copy.deepcopy(self.stored()['items']);items['_supplementary_conditions']['clauses'][0]['text']='Later unrelated draft edit'
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.draft,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.state(key)['catalog'],original['catalog'])
        other=uuid.uuid4();other_items=copy.deepcopy(items);other_items['_lease_history']={'documents':[],'emails':[]};other_items.pop('_addendum_clause_registers',None)
        self.session.execute(text('INSERT INTO bil_lease_draft(id,property_id,manager_id,source_version,items,version,status,registration_token) SELECT :other,property_id,manager_id,source_version,CAST(:items AS jsonb),version,status,:token FROM bil_lease_draft WHERE id=:id'),{'other':other,'items':json.dumps(other_items),'token':uuid.uuid4(),'id':self.draft});self.session.commit()
        page=self.client.get(self.register_url(other));self.assertEqual(page.status_code,200)
        state=json.loads(html.fromstring(page.data).xpath('//*[@id="addendum-clause-data"]/text()')[0]);self.assertEqual(state['selected'],[])
        entry=original['catalog'][0];data={'csrf_token':self.csrf,'action':'addendum_clause_selection','clause_lease':key,'register_version':str(state['version']),'provision_id':entry['id'],'selected':'yes'}
        self.assertEqual(self.client.post(self.register_url(other),data=data).status_code,404)
        self.assertEqual(self.client.get(self.register_url().replace('/26/','/27/')).status_code,404)

    def ledger_original(self,identifier):return next(e for e in self.stored()['items']['_lease_history']['documents'] if e['id']==identifier)
    def ledger_latest(self):return self.stored()['items']['_lease_history']['documents'][-1]['id']
