"""Focused Special Conditions save/reopen using temporary local PostgreSQL fixtures."""
import copy,sys,types,unittest
from unittest.mock import patch
from lxml import html
import test_pm_lease_lessee_persistence as fixtures

def word_paragraphs(payload):
    from io import BytesIO
    from zipfile import ZipFile
    import re
    from lxml import etree as ET
    env={'BytesIO':BytesIO,'ZipFile':ZipFile,'ET':ET,'W':'http://schemas.openxmlformats.org/wordprocessingml/2006/main','re':re}
    fixtures.functions('app/program_billing/lease_preview.py',{'extract_blocks','paragraph_text'},env)
    with ZipFile(BytesIO(payload)) as archive:
        assert archive.testzip() is None
        for member in archive.namelist():ET.fromstring(archive.read(member))
        assert b'word/document.xml' in archive.read('_rels/.rels')
        assert b'wordprocessingml.document.main+xml' in archive.read('[Content_Types].xml')
    return [block['text'] for block in env['extract_blocks'](payload)]

class SpecialConditionsPersistenceTests(unittest.TestCase):
    stored=fixtures.LesseePersistenceTests.stored
    def setUp(self):
        fixtures.LesseePersistenceTests.setUp(self)
        standard=types.ModuleType('app.program_billing.lease_standard')
        fixtures.functions('app/program_billing/lease_standard.py',{'special_conditions'},standard.__dict__)
        self.legacy=patch.dict(sys.modules,{standard.__name__:standard});self.legacy.start();self.addCleanup(self.legacy.stop)
        self.url=self.url.replace('/parties?','/additional?')
    def payload(self,clauses):
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'action':'save','sc_editor_version':'1','sc_mode':'add','sc_count':str(len(clauses))}
        for index,(title,body) in enumerate(clauses,1):
            data.update({'sc_heading_type_'+str(index):'Custom','sc_custom_heading_'+str(index):title,'sc_text_'+str(index):body})
        return data
    def test_save_add_edit_reopen_and_preserve_other_particulars(self):
        before=copy.deepcopy(self.stored()['items'])
        clauses=[('Original Mixed Heading','Keep Body Case.')]
        for clauses in (clauses,clauses+[('Second Heading','Second Body.')],[('Edited Heading','Edited Body.'),('Second Heading','Second Body.')]):
            response=self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=self.payload(clauses))
            self.assertEqual(response.status_code,302)
            saved=self.stored()['items'];self.assertEqual([(row['heading'],row['text']) for row in saved['_supplementary_conditions']['clauses']],clauses)
            self.assertEqual({key:value for key,value in saved.items() if key!='_supplementary_conditions'},before)
            page=self.client.get(response.location,headers={'X-Test-Owner':'yes'});self.assertEqual(page.status_code,200)
            tree=html.fromstring(page.data);self.assertFalse(tree.xpath('//*[@id="capture-save"]'))
            self.assertTrue(tree.xpath('//*[@id="sc-entry"][@hidden][@disabled]'))
            self.assertEqual(len(tree.xpath('//*[@id="sc-save-clause"]')),1)
            for _,body in clauses:self.assertIn(body,page.text)
    def test_more_than_fifty_clauses_and_long_text_save_and_reopen(self):
        clauses=[('Heading '+str(index),'Body '+str(index)) for index in range(1,62)]
        clauses[-1]=('Long Heading','Mixed Case Body. '*1500)
        data=self.payload(clauses)
        response=self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=data)
        self.assertEqual(response.status_code,302)
        saved=self.stored()['items']['_supplementary_conditions']['clauses']
        self.assertEqual(len(saved),61);self.assertEqual(saved[-1]['text'],clauses[-1][1].strip())
        page=self.client.get(response.location,headers={'X-Test-Owner':'yes'});self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//*[@id="sc-clauses"]//input[starts-with(@name,"sc_text_")]')),61)
        self.assertFalse(tree.xpath('//*[@id="sc-text"]/@maxlength'))
        header=tree.xpath('//*[@class="sc-text-header"]')[0]
        self.assertEqual(header.xpath('./label/@for'),['sc-text'])
        self.assertEqual(header.xpath('.//button/@id'),['sc-save-clause','sc-cancel'])
        self.assertEqual(len(tree.xpath('//*[@id="sc-cancel"]')),1)
        from app.program_billing import lease_supplementary_conditions as sc
        self.assertEqual(len(sc.editor({},data)['clauses']),61)
    def test_input_integrity_checks_remain_without_clause_limits(self):
        from app.program_billing import lease_supplementary_conditions as sc
        data=self.payload([('Heading','Body')])
        for changes in ({'sc_count':'-1'},{'sc_count':'999999999999'},{'sc_heading_type_1':'Unknown'},{'sc_custom_heading_1':'a'*201},{'sc_text_1':'bad\x00text'},{'sc_text_1':''}):
            with self.assertRaises(ValueError):sc.submitted({**data,**changes},{})

    def export_url(self,property_id=26,draft_id=None):
        return '/billing/leases/property/'+str(property_id)+'/special-conditions.docx?draft_id='+str(draft_id or self.identifier)
    def test_word_export_uses_all_saved_clauses_verbatim_and_current_metadata(self):
        from io import BytesIO
        from sqlalchemy import event
        clauses=[('Mixed heading '+str(n),'  Exact body '+str(n)+'\n3.7 Internal numbering\n3.7 Repeated number\t& <text>  ') for n in range(61)]
        self.assertEqual(self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=self.payload(clauses)).status_code,302)
        before=copy.deepcopy(dict(self.stored()))
        statements=[]
        def record(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
        event.listen(self.engine,'before_cursor_execute',record)
        try:
            # URL-supplied unsaved contents must never become part of this export.
            response=self.client.get(self.export_url()+'&sc_text_1=UNSAVED&blank=1',headers={'X-Test-Owner':'yes'})
        finally:event.remove(self.engine,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200)
        self.assertTrue(all(statement.lstrip().upper().startswith('SELECT') for statement in statements),statements)
        self.assertEqual(response.mimetype,'application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        self.assertIn('attachment;',response.headers['Content-Disposition']);self.assertIn('.docx',response.headers['Content-Disposition'])
        self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        paragraphs=word_paragraphs(response.data)
        self.assertIn('Property: Synthetic property (ID 26)',paragraphs)
        self.assertIn('Lease draft ID: '+str(self.identifier),paragraphs)
        self.assertIn('Draft version: '+str(before['version']),paragraphs)
        self.assertTrue(any(p.startswith('Export date: ') and p.endswith('(Africa/Johannesburg)') for p in paragraphs))
        start=next(i for i,p in enumerate(paragraphs) if p.startswith('SC-01'))
        expected=[]
        for index,clause in enumerate(before['items']['_supplementary_conditions']['clauses'],1):expected.extend(['SC-%02d — '%index+clause['heading'],clause['text']])
        self.assertEqual(paragraphs[start:],expected);self.assertNotIn('UNSAVED',response.data.decode('latin1'))
        self.assertEqual(dict(self.stored()),before)
        page=self.client.get(self.url,headers={'X-Test-Owner':'yes'});tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//a[@class="sc-export-word"]'))
    def test_word_export_rejects_wrong_user_property_draft_and_missing_selection(self):
        import uuid,json
        from sqlalchemy import text
        wrong=uuid.uuid4()
        self.session.execute(text("INSERT INTO bil_property VALUES (27,'Other property','Other address',7,false)"))
        self.session.execute(text("INSERT INTO bil_lease_draft(id,property_id,manager_id,status,source_version,items,version,updated_at) VALUES (:id,27,7,'draft',:source,CAST(:items AS jsonb),1,now())"),{'id':wrong,'source':self.catalog['version'],'items':json.dumps(self.initial)})
        self.session.commit()
        for url,headers,status in ((self.export_url(),{},403),(self.export_url(27),{'X-Test-Owner':'yes'},404),(self.export_url(draft_id=wrong),{'X-Test-Owner':'yes'},404),('/billing/leases/property/26/special-conditions.docx',{'X-Test-Owner':'yes'},400),(self.export_url(draft_id='not-a-uuid'),{'X-Test-Owner':'yes'},404)):
            self.assertEqual(self.client.get(url,headers=headers).status_code,status)
        self.user.id=8
        self.assertEqual(self.client.get(self.export_url(),headers={'X-Test-Owner':'yes'}).status_code,404)
        self.user.id=7
        self.assertEqual(self.client.post(self.export_url(),headers={'X-Test-Owner':'yes'},data={'csrf_token':self.csrf}).status_code,405)
    def test_word_export_retains_tenant_names_numbers_and_legacy_saved_conditions(self):
        from io import BytesIO
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from app.program_billing.lease_sc_export import build_word
        items={'_parties':{'lessees':[{'legal_name':'mIXed Tenant NPC'}]},'_supplementary_conditions':{'mode':'none','clauses':[{'number':'SC-09','heading':'mIXed Heading','text':'Same wording.'},{'number':'SC-09','heading':'mIXed Heading','text':'Same wording.'}]},'sc-04':{'value':'Original legacy condition.'},'_additional_clauses':[{'number':42,'text':'42.1 Original wording.'}],'_amounts':{'utilities_allocation_text':'Utilities exactly saved.'}}
        before=copy.deepcopy(items)
        payload=build_word({'id':26,'name':'Almond House'},{'id':'saved-draft','version':7,'items':items},datetime(2026,10,25,tzinfo=ZoneInfo('Africa/Johannesburg')))
        paragraphs=word_paragraphs(payload)
        self.assertIn('Tenant: mIXed Tenant NPC',paragraphs)
        self.assertEqual(paragraphs.count('SC-09 — mIXed Heading'),2);self.assertEqual(paragraphs.count('Same wording.'),2)
        for value in ('sc-04','Original legacy condition.','42','42.1 Original wording.','Utilities exactly saved.'):self.assertIn(value,paragraphs)
        self.assertEqual(items,before)
        items['_supplementary_conditions']['clauses']=[{'heading':'  Unchanged Heading  ','text':'  Original body.\n\n3.1 Internal number\t  ' }]
        check=word_paragraphs(build_word({'id':26,'name':'Almond House'},{'id':'saved-draft','version':7,'items':items},datetime(2026,10,25,tzinfo=ZoneInfo('Africa/Johannesburg'))))
        self.assertIn('SC-01 —   Unchanged Heading  ',check);self.assertIn('  Original body.\n\n3.1 Internal number\t  ',check)
        items['_supplementary_conditions']['clauses'][0]['text']='Invalid\x00text'
        with self.assertRaises(ValueError):build_word({'id':26,'name':'Almond House'},{'id':'saved-draft','version':7,'items':items},datetime(2026,10,25,tzinfo=ZoneInfo('Africa/Johannesburg')))
    def test_word_export_is_in_memory_and_no_templates_or_storage_are_used(self):
        from app.program_billing import lease_sc_export
        # Import before the open guard; export must need no files, templates or R2.
        with patch('builtins.open',side_effect=AssertionError('Export attempted disk access')):
            response=self.client.get(self.export_url(),headers={'X-Test-Owner':'yes'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.stored()['items'],self.initial)

    def test_stale_unauthorized_and_csrf_saves_do_not_change_data(self):
        data=self.payload([('New Heading','New Body.')]);before=copy.deepcopy(self.stored()['items'])
        for headers,payload in (({},data),({'X-Test-Owner':'yes'},{**data,'csrf_token':''}),({'X-Test-Owner':'yes'},{**data,'version':'0'})):
            response=self.client.post(self.url,headers=headers,data=payload)
            self.assertIn(response.status_code,(400,403,409));self.assertEqual(self.stored()['items'],before)

if __name__=='__main__':unittest.main()
