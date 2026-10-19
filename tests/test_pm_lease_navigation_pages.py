"""Lease submenu and honest logical-page text navigation."""
import copy,io,unittest
from zipfile import ZipFile
from lxml import html
import test_pm_lease_tenant_page_selection as fixture

class LeaseNavigationPagesTests(fixture.TenantPageSelectionTests):
    from test_pm_lease_live_preview import LiveTextPreviewTests
    setUp=LiveTextPreviewTests.setUp
    def test_submenu_preserves_context_forms_pdf_word_and_no_writes(self):
        self.schema();self.owners()
        before=dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one())
        response=self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft));self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]'))
        self.assertEqual(len(tree.xpath('//a[text()="Back to Property Management"]')),1)
        links=tree.xpath('//nav[@aria-label="Lease submenu"]/a')
        self.assertEqual([a.text_content().strip() for a in links],['1. Parties','2. Premises','3. Period','4. Amounts','5. Special Conditions','6. View Lease','7. Addendum','8. Lease History'])
        for link,group in zip(links[:5],('parties','premises','period','amounts','conditions')):
            self.assertEqual(link.get('href'),self.route('particulars/'+group))
            self.assertEqual(self.client.get(link.get('href')).status_code,200)
        view=self.client.get(links[5].get('href'));self.assertEqual(view.status_code,200)
        viewer=html.fromstring(view.get_data(as_text=True))
        pages=viewer.xpath('//*[@data-lease-page]');self.assertGreater(len(pages),2)
        self.assertEqual(len([p for p in pages if 'hidden' not in p.attrib]),1)
        self.assertTrue(viewer.xpath('//button[@id="lease-previous" and @hidden]'))
        self.assertEqual(viewer.xpath('//input[@id="lease-page-number"]')[0].get('max'),str(len(pages)))
        self.assertNotIn('Logical pages follow',view.get_data(as_text=True))
        self.assertNotIn('Saved draft revision',view.get_data(as_text=True))
        self.assertFalse(viewer.xpath('//nav[@aria-label="Lease submenu"]'))
        self.assertFalse(viewer.xpath('//a[contains(@href,"/particulars/") or contains(@href,"/generate-pdf") or contains(@href,"/review?")]'))
        self.assertNotIn('LIVE PREVIEW',viewer.xpath('//article')[0].text_content())
        self.assertFalse(viewer.xpath('//article//a | //article//details'))
        self.assertNotIn('Saved form values used on this logical page',view.get_data(as_text=True))
        self.assertFalse(viewer.xpath('//*[@id="lease-page-status"]'))
        toolbar=viewer.xpath('//*[@id="lease-page-controls"]')[0]
        self.assertIn('flex-nowrap',toolbar.get('class'))
        zoom=toolbar.xpath('./div[@id="lease-zoom-controls"]')[0]
        self.assertIn('ml-auto',zoom.get('class'))
        self.assertFalse(zoom.xpath('./button[@id="lease-zoom-fit"]'))
        for page in pages:
            self.assertIn('width:100%;max-width:100%',page.get('style'))
        for table in viewer.xpath('//article//table'):self.assertIn('table-layout:fixed',table.get('style'))
        for button in toolbar.xpath('.//button'):self.assertIn('bg-blue-50',button.get('class'))
        self.assertNotIn('Play',view.get_data(as_text=True))
        pdf=self.client.get(self.route('generate-pdf'));self.assertEqual(pdf.status_code,200)
        self.assertTrue(pdf.mimetype=='text/html');self.assertIn('PDF generation not yet available.',pdf.get_data(as_text=True))
        self.assertFalse(pdf.data.startswith((b'%PDF-',b'PK')))
        back=viewer.xpath('//a[text()="Back"]')[0].get('href')
        self.assertEqual(back,'/billing/leases?property_id=26&draft_id='+str(self.draft))
        self.assertEqual(self.client.get(back).status_code,200)
        self.assertFalse(tree.xpath('//a[text()="Generate Word"]'));word=self.route('review')
        self.assertIn('Stage 5 completion check',self.client.get(word).get_data(as_text=True))
        self.assertEqual(dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one()),before)
        self.sign_in(8)
        for link in links:self.assertEqual(self.client.get(link.get('href')).status_code,404)

    def test_logical_pages_preserve_all_text_sources_and_explicit_breaks(self):
        from app.program_billing import lease_preview as preview
        from app.program_billing.lease_generation import load_master
        self.schema();self.owners()
        source=preview.extract_blocks(load_master());merged=preview.extract_blocks(preview.merged_preview(self.stored(),self.uploaded_master,self.uploaded_digest))
        pages=preview.logical_pages(merged,source)
        self.assertEqual(''.join(preview.block_text(b) for b in merged),''.join(preview.block_text(b) for page in pages for b in page['blocks']))
        tokens={t for page in pages for t in page['tokens']}
        self.assertEqual(len(tokens),37);self.assertEqual(tokens,set(preview.placeholder_sources()))
        xml='<w:document xmlns:w="'+preview.W+'"><w:body><w:p><w:r><w:t>Before</w:t><w:br w:type="page"/><w:t>After</w:t></w:r></w:p><w:p><w:pPr><w:pageBreakBefore/></w:pPr><w:r><w:t>Next</w:t></w:r></w:p></w:body></w:document>'
        stream=io.BytesIO()
        with ZipFile(stream,'w') as z:z.writestr('word/document.xml',xml)
        blocks=preview.extract_blocks(stream.getvalue())
        pages=preview.logical_pages(blocks,blocks)
        self.assertEqual([''.join(preview.block_text(b) for b in p['blocks']) for p in pages],['Before','After','Next'])

    def test_clause_1_2_1_uses_saved_300_area_without_master_changes(self):
        from test_pm_lease_premises_offline import PremisesOfflineTests
        owners,ids=self.prepare();self.assertEqual(self.select([ids[0]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        values=PremisesOfflineTests.form(self);values['leased_area_sqm']='300'
        self.assertEqual(self.post('premises',values).status_code,302)
        before=dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one())
        view=self.client.get(self.route('view'));self.assertEqual(view.status_code,200)
        tree=html.fromstring(view.get_data(as_text=True))
        clauses=tree.xpath('//article//p[starts-with(normalize-space(),"1.2.1 ")]')
        self.assertEqual(len(clauses),1)
        self.assertIn('approximately 300 square metres',clauses[0].text_content())
        self.assertEqual(dict(self.sql('SELECT * FROM bil_lease_draft').mappings().one()),before)
