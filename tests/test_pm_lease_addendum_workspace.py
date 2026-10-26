"""Three workspaces and exact selected-reference PDF previews, isolated PostgreSQL."""
import copy,io,json,unittest
from lxml import html
from pypdf import PdfReader
import test_pm_lease_addendum_selection as fixture

class AddendumWorkspaceTests(unittest.TestCase):
    setUp=fixture.AddendumSelectionTests.setUp
    stored=fixture.AddendumSelectionTests.stored
    payload=fixture.AddendumSelectionTests.payload
    sql=fixture.AddendumSelectionTests.sql
    signed=fixture.AddendumSelectionTests.signed
    pdf=fixture.AddendumSelectionTests.pdf
    state=fixture.AddendumSelectionTests.state
    register_url=fixture.AddendumSelectionTests.register_url
    choose=fixture.AddendumSelectionTests.choose
    def preview(self,**changes):
        data={'csrf_token':self.csrf,'action':'addendum_preview','original_id':self.state()['lease_key'] if self.state()['lease_key']!='draft' else '', 'subject':'Rental Adjustment','wording':'One amendment covers multiple provisions.\nKeep this exact second paragraph.'};data.update(changes)
        return self.client.post(self.register_url(),data=data)
    def test_workspace_layout_compact_sources_one_content_and_output_link(self):
        page=self.client.get(self.register_url());self.assertEqual(page.status_code,200,page.text);tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="addendum-workflow"]/button/text()'),['Select Clause','Create Addendum','View Addendum','Help'])
        panels=tree.xpath('//*[@data-addendum-panel]');self.assertEqual([p.get('data-addendum-panel') for p in panels if p.get('hidden') is None],['select'])
        self.assertFalse(tree.xpath('//*[@id="addendum-clause-catalog"]'))
        self.assertEqual(len(tree.xpath('//*[@id="addendum-create-form"]//input[@name="subject"]')),0);self.assertEqual(len(tree.xpath('//*[@id="addendum-create-form"]//textarea[@name="wording"]')),1)
        self.assertIn('Are you not amending a clause found in the Lease Agreement?',page.text);self.assertIn('Continue Without Selecting a Clause',page.text)
        self.assertTrue(tree.xpath('//*[@id="addendum-pdf-document" and @data-lease-pdf-viewer]'));self.assertEqual(tree.xpath('//*[@id="addendum-pdf-document"]//button/text()'),['Zoom Out','Zoom In','Previous','Next'])
        self.assertIn('To print, download or email your completed Addendum, go to',page.text)
    def test_legacy_table_selections_are_retained_but_not_used_as_pdf_selections(self):
        self.signed();original=copy.deepcopy(self.stored()['items']['_lease_history']['documents'][0]);state=self.state()
        core=next(e for e in state['catalog'] if e['source']=='core' and e['reference']=='5.1');sc=next(e for e in state['catalog'] if e['source']=='special_conditions')
        self.assertEqual(self.choose(core).status_code,200);self.assertEqual(self.choose(sc).status_code,200);before=copy.deepcopy(dict(self.stored()))
        result=self.preview();self.assertEqual(result.status_code,200,result.data[:200]);self.assertIn('inline',result.headers['Content-Disposition']);self.assertIn('no-store',result.headers['Cache-Control'])
        doc=PdfReader(io.BytesIO(result.data));body=''.join(p.extract_text() for p in doc.pages)
        for value in ('ADDENDUM 01','GENERAL ADDENDUM TO AGREEMENT OF LEASE',original['lease_reference'],'SUBJECT: GENERAL AMENDMENT','One amendment covers multiple provisions.','Keep this exact second paragraph.'):
            self.assertIn(value,body)
        self.assertIn('SIGNATURES',doc.pages[-1].extract_text());self.assertNotIn('SIGNATURES',doc.pages[0].extract_text());self.assertEqual(dict(self.stored()),before)
        import os
        if os.getenv('AIT_ADDENDUM_SAMPLE')=='1':
            from pathlib import Path
            import fitz
            folder=Path('output/pdf');folder.mkdir(parents=True,exist_ok=True);(folder/'Addendum_Workspace_Sample.pdf').write_bytes(result.data)
            sample=fitz.open(stream=result.data,filetype='pdf')
            for index,page in enumerate(sample):page.get_pixmap().save(str(folder/('addendum-workspace-page-'+str(index+1)+'.png')))
        result=self.preview(action='create_pdf');self.assertEqual(result.status_code,302,result.text);saved=self.stored()['items']['_lease_history']['documents'][-1]
        self.assertEqual(saved['addendum_number'],1);self.assertEqual(saved['subject'],'GENERAL AMENDMENT');self.assertEqual(saved['selected_provisions'],[]);self.assertEqual(self.stored()['items']['_lease_history']['documents'][0],original)
        output=self.client.get(self.endpoint);self.assertEqual(output.status_code,200,output.text);self.assertIn('Saved Addendums',output.text);self.assertIn(saved['id'],output.text)
        second=self.preview();self.assertIn('ADDENDUM 02',''.join(p.extract_text() for p in PdfReader(io.BytesIO(second.data)).pages))
    def test_general_preview_has_no_invented_clauses_and_invalid_access_does_not_save(self):
        before=copy.deepcopy(dict(self.stored()));result=self.preview();self.assertEqual(result.status_code,200)
        body=''.join(p.extract_text() for p in PdfReader(io.BytesIO(result.data)).pages);self.assertIn('GENERAL ADDENDUM TO AGREEMENT OF LEASE',body);self.assertNotIn('NOTICE REGARDING CHANGES TO CLAUSES:',body);self.assertNotIn('SC-06',body);self.assertIn(str(self.draft),body)
        self.assertEqual(dict(self.stored()),before);self.assertEqual(self.preview(wording='').status_code,400);self.assertEqual(self.preview(csrf_token='').status_code,400)
        self.client.environ_base.pop('HTTP_X_TEST_OWNER');self.assertEqual(self.preview().status_code,403)
