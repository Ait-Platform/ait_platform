"""Saved-data mail-merge text viewer: placement, authorization and no writes."""
import copy,io,unittest
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree,html
from sqlalchemy import event,text
import test_pm_lease_tenant_page_selection as fixture

class LiveTextPreviewTests(fixture.TenantPageSelectionTests):
    def viewer(self):return self.client.get(self.route('view'))

    def test_saved_part_a_placement_missing_tokens_and_no_writes(self):
        from app.program_billing import lease_preview as preview
        from app.program_billing.lease_generation import load_master
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0],ids[2]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        before=dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one())
        statements=[]
        def capture(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:response=self.client.get(self.route('view')+'&lessee_legal_name=UNSAVED-VALUE')
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,200)
        self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','DELETE','CREATE','ALTER','DROP')) for s in statements))
        self.assertEqual(dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one()),before)
        self.assertIn('no-store',response.headers['Cache-Control'])
        tree=html.fromstring(response.get_data(as_text=True));visible=tree.xpath('//article[@id="lease-merged-text"]')[0].text_content()
        for value in ('First Natural Tenant','Contracting Company','Company Representative','[MONTHLY RENT INITIAL]','[EXPIRY DATE]'):
            self.assertIn(value,visible)
        self.assertNotIn('UNSAVED-VALUE',visible);self.assertNotIn('Second Natural Tenant',visible)
        self.assertTrue(tree.xpath('//article//table'))
        # Every original body paragraph stays in order, including table paragraphs;
        # each saved value replaces precisely its existing master token.
        def paragraphs(docx):
            with ZipFile(io.BytesIO(docx)) as archive:root=etree.fromstring(archive.read('word/document.xml'))
            body=root.find('{'+preview.W+'}body')
            return [preview.paragraph_text(p) for p in body.iter('{'+preview.W+'}p')]
        values=self.workflow_module.stage5_placeholder_values(self.stored())
        for token in self.workflow_module.SIGNATURE_TOKENS:
            if values.get(token)=='________________':values[token]=''
        expected=[]
        for paragraph in paragraphs(load_master()):
            for token,value in values.items():
                if value.strip().strip('_') and '[' not in value:paragraph=paragraph.replace(token,value)
            expected.append(paragraph)
        actual=paragraphs(preview.merged_preview(self.stored()))
        self.assertEqual(actual[1:],expected)
        self.assertEqual(self.generate().status_code,400)  # Download completion gate preserved.

    def test_saved_update_refresh_and_escaped_text(self):
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        self.assertIn('First Natural Tenant',self.viewer().get_data(as_text=True))
        self.assertEqual(self.select([ids[2]]).status_code,302)
        response=self.viewer();visible=html.fromstring(response.get_data(as_text=True)).xpath('//article')[0].text_content()
        self.assertIn('Contracting Company',visible);self.assertNotIn('First Natural Tenant',visible)
        # Escaping also applies to values persisted before this viewer was introduced.
        items=copy.deepcopy(self.stored());items['_parties']['lessee']['legal_name']='<script>alert(1)</script>'
        items['_parties']['lessees'][0]['legal_name']='<script>alert(1)</script>'
        import json
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()
        response=self.viewer();self.assertEqual(response.status_code,200)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;',response.get_data(as_text=True))
        self.assertEqual(html.fromstring(response.get_data(as_text=True)).xpath('//article//script'),[])

    def test_authorization_missing_draft_and_merge_failure(self):
        self.schema();self.owners()
        self.assertEqual(self.client.get('/billing/leases/property/26/view').status_code,400)
        self.assertEqual(self.client.get('/billing/leases/property/26/view?draft_id=invalid').status_code,404)
        before=copy.deepcopy(self.stored())
        with patch('app.program_billing.lease_preview.merged_preview',side_effect=ValueError('invalid master')):
            response=self.viewer()
        self.assertEqual(response.status_code,503);self.assertIn('saved lease text could not be displayed',response.get_data(as_text=True))
        self.assertEqual(self.stored(),before)
        self.sign_in(8);self.assertEqual(self.viewer().status_code,404)

    def test_complete_text_view_uses_generate_word_builder_unchanged(self):
        from app.program_billing import lease_preview as preview
        from app.program_billing.lease_generation import generate_review
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0],ids[2]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302);self.complete_other_parts()
        items=copy.deepcopy(self.stored());values=self.workflow_module.stage5_placeholder_values(items)
        warnings=self.workflow_module.parties_generation_warnings(items)
        self.assertEqual(preview.merged_preview(items),generate_review(values,incomplete_particulars=bool(warnings),particulars_warnings=warnings)['docx'])
        self.assertEqual(self.viewer().status_code,200);self.assertEqual(self.stored(),items)
        self.assertEqual(self.generate().status_code,200)

    def test_extract_paragraph_heading_and_nested_table_order(self):
        from app.program_billing.lease_preview import extract_blocks,W
        xml='<w:document xmlns:w="'+W+'"><w:body><w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>Heading</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>First</w:t><w:br/><w:t>line two</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>Nested</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:tc><w:tc><w:p><w:r><w:t>Second</w:t></w:r></w:p></w:tc></w:tr></w:tbl><w:p><w:r><w:t>Last</w:t></w:r></w:p></w:body></w:document>'
        stream=io.BytesIO()
        with ZipFile(stream,'w') as archive:archive.writestr('word/document.xml',xml)
        blocks=extract_blocks(stream.getvalue())
        self.assertEqual([b['kind'] for b in blocks],['heading','table','paragraph'])
        self.assertEqual(blocks[1]['rows'][0][0][0]['text'],'First\nline two')
        self.assertEqual(blocks[1]['rows'][0][0][1]['rows'][0][0][0]['text'],'Nested')
        self.assertEqual(blocks[1]['rows'][0][1][0]['text'],'Second')
        self.assertEqual(blocks[-1]['text'],'Last')

    def test_workspace_review_opens_viewer_and_word_keeps_checklist(self):
        self.schema();self.owners()
        before=dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one())
        response=self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft))
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        link=tree.xpath('//nav[@aria-label="Lease submenu"]/a[normalize-space()="6. View Lease"]')[0].get('href')
        self.assertEqual(link,self.route('view')+'&revision='+str(self.sql('SELECT version FROM bil_lease_draft').scalar()))
        self.assertIn('View the developing lease with saved particulars inserted into the document.',response.get_data(as_text=True))
        viewer=self.client.get(link);self.assertEqual(viewer.status_code,200)
        tree=html.fromstring(viewer.get_data(as_text=True))
        word=tree.xpath('//a[text()="Generate Word"]')[0].get('href')
        self.assertEqual(word,self.route('review'))
        checklist=self.client.get(word);self.assertEqual(checklist.status_code,200)
        self.assertIn('Stage 5 completion check',checklist.get_data(as_text=True))
        self.assertEqual(dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one()),before)
        self.sign_in(8)
        self.assertEqual(self.client.get(link).status_code,404)
        self.assertEqual(self.client.get(word).status_code,404)
        self.assertEqual(self.client.get('/billing/leases?property_id=26').status_code,404)

    def test_workspace_without_draft_instructs_save_without_creating(self):
        self.schema();self.owners();self.sql('DELETE FROM bil_lease_draft');self.session.commit()
        response=self.client.get('/billing/leases?property_id=26');self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertFalse(tree.xpath('//nav[@aria-label="Lease submenu"]/a[normalize-space()="6. View Lease"]'))
        self.assertIn('Save particulars first, then view the developing lease.',response.get_data(as_text=True))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),0)
