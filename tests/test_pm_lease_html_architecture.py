"""HTML-first Lease Kit and on-demand PDFs, using synthetic particulars and offline Core bytes."""
import copy,hashlib,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
from flask import Flask,abort,request,render_template
from jinja2 import ChoiceLoader,DictLoader,FileSystemLoader
from lxml import html
import fitz
ROOT=Path(__file__).resolve().parents[1]

class HtmlLeaseKitTests(unittest.TestCase):
    def setUp(self):
        mods={}
        for name,path in (('app',ROOT/'app'),('app.program_billing',ROOT/'app/program_billing'),('app.utils',ROOT/'app/utils')):
            module=types.ModuleType(name);module.__path__=[str(path)];mods[name]=module
        context=types.ModuleType('app.program_billing.lease_certificate_view')
        management=types.ModuleType('app.program_billing.lease_management');management.render_pm_template=render_template
        mods[context.__name__]=context;mods[management.__name__]=management
        self.modules=patch.dict(sys.modules,mods);self.modules.start();self.addCleanup(self.modules.stop)
        from app.program_billing import lease_kit,lease_kit_pdf,lease_signing
        self.kit=lease_kit;self.pdf=lease_kit_pdf;self.signing=lease_signing
        self.items={'s1-01':{'value':'SYNTHETIC-LEASE'},'_contracting_lessors':{'identities':[{'type':'individual','name':'Owner Person','identity_number':'OWNER-1'}]},
            '_parties':{'lessees':[{'party_type':'natural_person','tenant_id':11,'legal_name':'Natural Person','identity_number':'NAT-11'},
                {'party_type':'juristic_person','tenant_id':22,'legal_name':'Entity Company','identity_number':'REG-22','signatory_name':'Entity Director','signatory_identity_number':'REP-33'}]},
            '_supplementary_conditions':{'mode':'add','clauses':[{'heading':'Saved special clause','text':'The exact saved condition wording.'}]}}
        for index,party in enumerate(lease_signing.rows(self.items)):
            self.items.setdefault(lease_signing.KEY,{})[party['key']]={'place':'Signing City '+str(index),'date':'2026-11-'+str(10+index)}
        self.original=copy.deepcopy(self.items)
        def selected(property_id,allow_blank=False):
            if request.headers.get('X-Test-Owner')!='yes':abort(403)
            if property_id!=26 or request.args.get('draft_id')!='draft-one':abort(404)
            return {'id':26,'name':'Synthetic Property','address':'Synthetic Address'}, {'id':'draft-one','version':1,'items':self.items},copy.deepcopy(self.items),[],[]
        context.context=selected
        app=Flask('html-first-lease-kit',root_path=str(ROOT/'app'));app.secret_key='synthetic-only'
        app.jinja_loader=ChoiceLoader([DictLoader({'program_billing/lease_workflow_base.html':'{% block workspace %}{% endblock %}'}),FileSystemLoader(ROOT/'templates')])
        for name in ('lease_kit_pdf','lease_kit_email','lease_live_view','lease_certificate_edit','pm_lease_management'):
            app.add_url_rule('/'+name+'/<int:property_id>',endpoint='billing_bp.'+name,view_func=lambda **kw:'')
        app.add_url_rule('/view/<int:property_id>',view_func=lease_kit.view)
        self.app=app;self.client=app.test_client();self.headers={'X-Test-Owner':'yes'}

    def context(self,component):
        return self.app.test_request_context('/?draft_id=draft-one&component='+component,headers=self.headers)

    def test_all_views_use_saved_html_and_only_core_uses_pdf_viewer(self):
        with patch.object(self.pdf,'core_bytes',side_effect=AssertionError('View fetched private PDF')),patch.object(self.pdf,'build_pdf',side_effect=AssertionError('View generated PDF')):
            for component,_ in self.kit.COMPONENTS:
                response=self.client.get('/view/26?draft_id=draft-one&component='+component,headers=self.headers)
                self.assertEqual(response.status_code,200)
                tree=html.fromstring(response.data)
                self.assertEqual(tree.xpath('//*[@data-lease-component]/@data-lease-component'),list(self.kit.ORDER) if component=='lease' else [component])
                self.assertEqual(len(tree.xpath('//*[@id="lease-kit-canvas"]')),int(component in ('core','lease')))
                self.assertNotIn('target="_blank"',response.text);self.assertNotIn('<iframe',response.text)
                if component in ('core','lease'):
                    self.assertIn('component=core',tree.xpath('//*[@id="lease-kit-document"]/@data-pdf-url')[0])
                if component in ('certificate','signature','lease'):
                    for value in (('SIGNING CITY 0','SIGNING CITY 1','SIGNING CITY 2','10th day of NOVEMBER 2026','11th day of NOVEMBER 2026','12th day of NOVEMBER 2026') if component=='signature' else ('Signing City 0','Signing City 1','Signing City 2','2026-11-10','2026-11-11','2026-11-12')):
                        self.assertIn(value,response.text)
                    self.assertNotIn('certificate-party-warning',response.text)
                    self.assertNotRegex(response.text,r'\[LESSEE_\d+_')
        self.assertEqual(self.items,self.original)
        for url in ('/view/26?draft_id=draft-one','/view/27?draft_id=draft-one','/view/26?draft_id=wrong'):
            self.assertIn(self.client.get(url,headers=self.headers if '/27' in url or 'wrong' in url else {}).status_code,(403,404))

    def test_six_on_demand_pdfs_match_shared_html_and_full_lease_page_order(self):
        payloads={};texts={};core=self.pdf.CORE.read_bytes()
        with patch.object(self.pdf,'core_bytes',return_value=core):
            for component,_ in self.kit.COMPONENTS:
                with self.context(component):
                    context=self.kit.preview_context(26)
                    payload=self.pdf.build_pdf(component,context)
                    self.assertEqual(hashlib.sha256(payload).digest(),hashlib.sha256(self.pdf.build_pdf(component,context)).digest(),'Email token must address reproducible PDF bytes')
                payloads[component]=payload
                document=fitz.open(stream=payload,filetype='pdf');texts[component]=[page.get_text() for page in document]
                self.assertTrue(all(text.strip() for text in texts[component]))
                if component!='core':
                    for index,page in enumerate(document):
                        # Full Lease contains the immutable Core, whose own content is unchanged.
                        if component=='lease' and len(texts['cover'])+len(texts['certificate'])<=index<len(texts['cover'])+len(texts['certificate'])+9:continue
                        footer=page.get_text(clip=fitz.Rect(0,page.rect.height-40,page.rect.width,page.rect.height))
                        self.assertNotIn('DRAFT',footer)
                        self.assertNotIn('Required representative details are missing',page.get_text())
            self.assertEqual(payloads['core'],core);self.assertEqual(len(texts['core']),9)
            self.assertEqual(len(texts['lease']),sum(len(texts[key]) for key in self.kit.ORDER))
            for index,value in enumerate(texts['lease']):
                if index==0:self.assertNotIn('Page 1',value)
                else:self.assertTrue(value.rstrip().endswith('Page '+str(index)))
            for component in ('certificate','signature'):
                text=''.join(texts[component])
                for value in ((('SIGNING CITY 0','SIGNING CITY 1','SIGNING CITY 2','10th day of NOVEMBER 2026','11th day of NOVEMBER 2026','12th day of NOVEMBER 2026') if component=='signature' else ('Signing City 0','Signing City 1','Signing City 2','2026-11-10','2026-11-11','2026-11-12'))+('Natural Person','Entity Company','Entity Director')):
                    self.assertIn(value,text)
            self.assertIn('The exact saved condition wording.',''.join(texts['special_conditions']))
            self.assertIn('SAVED SPECIAL CLAUSE',''.join(texts['special_conditions']))
            self.assertIn('SAVED SPECIAL CLAUSE',''.join(texts['lease']))
            for component in ('special_conditions','lease'):
                page=self.client.get('/view/26?draft_id=draft-one&component='+component,headers=self.headers)
                tree=html.fromstring(page.data)
                self.assertTrue(tree.xpath('//h3[contains(.,"SAVED SPECIAL CLAUSE")]'))
                self.assertIn('The exact saved condition wording.',page.text)
        self.assertEqual(self.items,self.original)

    def test_legacy_and_editor_conditions_remain_visible_without_bypassing_pdf_review(self):
        self.items['sc-01']={'value':'Preserved legacy condition.'}
        before=copy.deepcopy(self.items)
        for component in ('special_conditions','lease'):
            response=self.client.get('/view/26?draft_id=draft-one&component='+component,headers=self.headers)
            self.assertEqual(response.status_code,200)
            self.assertIn('Preserved legacy condition.',response.text)
            self.assertIn('The exact saved condition wording.',response.text)
            with self.context(component):
                with self.assertRaises(ValueError):self.pdf.build_pdf(component,self.kit.preview_context(26))
        self.assertEqual(self.items,before)

    def test_empty_selection_stays_empty_and_none_conditions_remain_printable(self):
        self.items['_parties']['lessees']=[]
        self.items['_parties']['lessee']={'legal_name':'Removed Legacy Party','identity_number':'OLD-1'}
        self.items['_supplementary_conditions']={'mode':'none','clauses':[]}
        with self.context('certificate'):
            context=self.kit.preview_context(26)
            self.assertEqual(context['party_blocks'][1]['parties'],[])
            self.assertTrue(context['states']['special_conditions']['printable'])
            page=render_template('program_billing/lease_kit_view.html',**context)
            self.assertNotIn('Removed Legacy Party',page)
            document=fitz.open(stream=self.pdf.build_pdf('special_conditions',context),filetype='pdf')
            self.assertIn('None.',''.join(p.get_text() for p in document))

    def test_legal_presentation_formats_headings_and_utility_register_without_mutating_items(self):
        import app.program_billing.lease_certificate_view as context_module
        original_context=context_module.context
        def almond(property_id,allow_blank=False):
            prop,draft,items,owners,tenants=original_context(property_id,allow_blank)
            return {**prop,'name':'Almond House'},draft,items,owners,tenants
        context_module.context=almond
        identifier='11111111-2222-3333-4444-555555555555'
        self.items['s1-01']={'value':'LEASE-'+identifier}
        self.items['_supplementary_conditions']['clauses']=[{'heading':'**Municipal Utility Register**','text':'### Municipal Utility Register\n| Service | Meter |\n| --- | --- |\n| Water | W-01 |\n| Electricity | E-02 |\n**1.1** Preserve exact obligations.'}]
        before=copy.deepcopy(self.items)
        with self.context('special_conditions'):
            context=self.kit.preview_context(26)
            self.assertEqual(context['property']['name'],'Almond');self.assertEqual(context['lease_reference'],'')
            page=render_template('program_billing/lease_kit_view.html',**context)
            tree=html.fromstring(page)
            self.assertEqual(tree.xpath('//table[@class="legal-utility-table"]//th/text()'),['Service','Meter'])
            self.assertIn('W-01',tree.xpath('//table[@class="legal-utility-table"]')[0].text_content())
            payload=self.pdf.build_pdf('special_conditions',context);document=fitz.open(stream=payload,filetype='pdf');text=''.join(p.get_text() for p in document)
            self.assertNotIn('**',text);self.assertNotIn('###',text);self.assertNotIn('| ---',text)
            for value in ('W-01','E-02','Preserve exact obligations.'):self.assertIn(value,text)
        for component in ('cover','certificate','signature'):
            with self.context(component):
                payload=self.pdf.build_pdf(component,self.kit.preview_context(26));text=''.join(p.get_text() for p in fitz.open(stream=payload,filetype='pdf'))
                self.assertNotIn(identifier,text);self.assertNotIn('Almond House',text)
        self.assertEqual(self.items,before)


    def test_financial_heading_and_formal_signature_statements(self):
        self.items['_parties']['lessees']=self.items['_parties']['lessees'][:1]
        self.items['_signing_particulars']={party['key']:{'place':'Warner Beach, Durban','date':'2026-10-24'} for party in self.signing.rows(self.items)}
        with self.context('signature'):
            context=self.kit.preview_context(26);page=render_template('program_billing/lease_signature_document.html',**context);payload=self.pdf.build_pdf('signature',context)
        tree=html.fromstring(page);parties=tree.xpath('//*[@class="signature-party"]');self.assertEqual(len(parties),2)
        statement='SIGNED at WARNER BEACH, DURBAN on this the 24th day of OCTOBER 2026'
        for party in parties:
            self.assertEqual(party.text_content().count(statement),1);self.assertEqual(party.text_content().count('Witness 1 - Name:'),1);self.assertEqual(len(party.xpath('.//table[contains(@class,"signature-witnesses")]//tr[2]/td[contains(.,"Signature:")]')),1);self.assertNotIn('Place of signing:',party.text_content());self.assertNotIn('Date of signing:',party.text_content())
        self.assertEqual([p.xpath('./h3/text()')[0] for p in parties],['FOR AND ON BEHALF OF THE LESSOR','LESSEE / AUTHORISED REPRESENTATIVE'])
        text=' '.join(' '.join(p.get_text() for p in fitz.open(stream=payload,filetype='pdf')).split());self.assertEqual(text.count(statement),2);self.assertEqual(text.count('Witness 1 - Name:'),2);self.assertEqual(text.count('Witness 2 - Name:'),2)
        with self.context('certificate'):
            certificate=render_template('program_billing/lease_kit_component.html',component_key='certificate',**self.kit.preview_context(26))
        self.assertIn('04 FINANCIAL',certificate);self.assertNotIn('FINANCIAL PARTICULARS',certificate)
        for value,day in (('2026-01-01','1st'),('2026-02-02','2nd'),('2026-03-03','3rd'),('2026-04-11','11th'),('2026-05-12','12th'),('2026-06-13','13th'),('2026-10-21','21st'),('2026-11-22','22nd'),('2026-12-23','23rd')):
            self.assertIn('the '+day+' day',self.signing.signing_statement('Actual Place',value))
        self.assertNotIn('2026',self.signing.signing_statement('',''))
        import os
        if os.getenv('AIT_ADDENDUM_FINAL_SAMPLE')=='1':
            folder=ROOT/'tmp/pdfs/addendum-final';folder.mkdir(parents=True,exist_ok=True);(folder/'lease-signatures.pdf').write_bytes(payload)
            for index,p in enumerate(fitz.open(stream=payload,filetype='pdf')):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(folder/('lease-signatures-'+str(index+1)+'.png')))

if __name__=='__main__':unittest.main()
