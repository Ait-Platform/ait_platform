"""Typed Certificate party display: synthetic snapshots only, no database or R2."""
import ast,copy,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
from flask import Flask,render_template,redirect,url_for,request
from jinja2 import FileSystemLoader,ChoiceLoader,DictLoader
from lxml import html
ROOT=Path(__file__).resolve().parents[1]

class CertificatePartyTests(unittest.TestCase):
    def setUp(self):
        mods={name:types.ModuleType(name) for name in ('app','app.program_billing')}
        mods['app'].__path__=[str(ROOT/'app')];mods['app.program_billing'].__path__=[str(ROOT/'app/program_billing')]
        self.patch=patch.dict(sys.modules,mods);self.patch.start();self.addCleanup(self.patch.stop)
        from app.program_billing import lease_certificate_parties,lease_kit,lease_certificate_form,lease_certificate
        self.parties=lease_certificate_parties;self.kit=lease_kit;self.form=lease_certificate_form;self.cert=lease_certificate
        self.app=Flask('certificate-party-tests');self.app.jinja_loader=ChoiceLoader([DictLoader({'program_billing/lease_workflow_base.html':'{% block workspace %}{% endblock %}'}),FileSystemLoader(ROOT/'templates')])

    def items(self,lessors=None,lessees=None):
        return {'_contracting_lessors':{'identities':lessors if lessors is not None else [{'owner_id':1,'type':'individual','name':'Alex Owner','identity_number':'OWNER-A'}]},
                '_parties':{'lessees':lessees if lessees is not None else [{'tenant_id':2,'party_type':'natural_person','legal_name':'Taylor Tenant','identity_number':'TENANT-A'}]}}

    def rendered(self,items,owners=(),tenants=()):
        before=copy.deepcopy(items)
        values=self.cert.document_values(items)
        sections=self.kit.certificate_display_rows(self.form.certificate_layout(*self.form.counts(items)),values,items,owners,tenants)
        with self.app.app_context():result=render_template('program_billing/lease_certificate_document.html',sections=sections,preview=True)
        self.assertEqual(items,before)
        return result,html.fromstring(result)

    def test_single_individuals_sign_personally_without_representative_slots(self):
        rendered,tree=self.rendered(self.items())
        self.assertEqual(tree.xpath('//div[contains(@class,"certificate-party-block")]/h4/text()'),['LESSORS','LESSEES'])
        self.assertEqual(tree.xpath('//tr[@class="certificate-party"]/th/text()'),['Lessor','Lessee'])
        self.assertNotIn('Representative',tree.text_content());self.assertNotIn('SIGNATORY',rendered)
        self.assertFalse(tree.xpath('//*[contains(@class,"certificate-party-warning")]'))

    def test_multiple_parties_preserve_source_order_even_similar_or_identical(self):
        lessors=[{'type':'individual','name':'Same Name','identity_number':'A'}, {}, {'type':'company','name':'Same Name','identity_number':'B','signatory_name':'Director B','signatory_identity_number':'REP-B'}]
        tenants=[{'party_type':'natural_person','legal_name':'Same Tenant','identity_number':'T-A'}, {},
                 {'party_type':'natural_person','legal_name':'Same Tenant','identity_number':'T-B'},
                 {'party_type':'natural_person','legal_name':'Same Tenant','identity_number':'T-B'}]
        _,tree=self.rendered(self.items(lessors,tenants))
        rows=tree.xpath('//tr[@class="certificate-party"]')
        self.assertEqual([r.find('th').text for r in rows],['Lessor','Lessor','Lessee','Lessee','Lessee'])
        identities=tree.xpath('//div[@class="certificate-party-identity"]/text()')
        self.assertEqual([text.strip() for text in identities],['Same Name | A','Same Name | B','Same Tenant | T-A','Same Tenant | T-B','Same Tenant | T-B'])
        self.assertIn('Director B',rows[1].text_content());self.assertNotIn('Director B',rows[0].text_content())

    def test_entity_lessee_keeps_representative_beneath_its_own_identity(self):
        items=self.items(lessees=[{'party_type':'juristic_person','legal_name':'Example Company','identity_number':'REG-1',
            'signatory_name':'Casey Director','signatory_identity_number':'REP-1','signatory_capacity':'Director','representative_email':'rep@example.com'}])
        _,tree=self.rendered(items)
        tenant=tree.xpath('//tr[@class="certificate-party"][th="Lessee"]')[0].text_content()
        for text in ('Example Company','REG-1','Casey Director','REP-1','Director','rep@example.com'):self.assertIn(text,tenant)
        self.assertLess(tenant.index('Example Company'),tenant.index('Casey Director'))
        self.assertNotIn('Required representative details are missing',tenant)

    def test_entities_on_both_sides_warn_when_representative_is_missing(self):
        items=self.items([{'type':'trust','name':'Example Trust','identity_number':'TRUST-1'}],[{'party_type':'juristic_person','legal_name':'Example NPC','identity_number':'NPC-1'}])
        rendered,tree=self.rendered(items)
        warnings=tree.xpath('//*[contains(@class,"certificate-party-warning")]/text()')
        self.assertEqual(sum('Required representative' in w for w in warnings),2)
        self.assertNotIn('[LESSOR_',rendered);self.assertNotIn('[LESSEE_',rendered)

    def test_individual_self_signatory_is_not_repeated_and_unknown_type_not_inferred(self):
        items=self.items(lessees=[{'party_type':'natural_person','legal_name':'Self','identity_number':'SELF-1','signatory_name':'Self','signatory_identity_number':'SELF-1'},
                                 {'legal_name':'Unknown Type','identity_number':'UNK-1','signatory_name':'Recorded Agent','signatory_identity_number':'AGENT-1'}])
        _,tree=self.rendered(items)
        rows=tree.xpath('//tr[@class="certificate-party"][th="Lessee"]')
        self.assertNotIn('Representative',rows[0].text_content())
        self.assertIn('Party type is not recorded',rows[1].text_content())
        self.assertIn('Recorded Agent',rows[1].text_content())
        self.assertNotIn('Required representative details are missing',rows[1].text_content())

    def test_saved_party_values_win_over_register_and_ambiguous_global_mandate_not_assigned(self):
        items=self.items([{'owner_id':1,'type':'company','name':'Saved Company','identity_number':'CO-1'}, {'owner_id':2,'type':'individual','name':'Other Lessor','identity_number':'ID-2'}])
        items['_parties']['lessor']={'signatory_name':'Group Agent','signatory_identity_number':'AGENT-1'}
        _,tree=self.rendered(items,owners=[{'owner_id':1,'type':'individual','name':'Changed Register','identity_number':'CHANGED'}])
        self.assertIn('Saved Company | CO-1',tree.text_content());self.assertNotIn('Changed Register',tree.text_content())
        self.assertEqual(tree.text_content().count('Group Agent'),1)
        self.assertIn('not linked to an individual Lessor',tree.text_content())
        self.assertIn('Required representative details are missing',tree.text_content())

    def test_single_company_lessor_uses_saved_lessor_representative(self):
        items=self.items([{'type':'company','name':'Lessor Company','identity_number':'CO-1'}])
        items['_parties']['lessor']={'signatory_name':'Lessor Director','signatory_identity_number':'REP-2','signatory_capacity':'Director'}
        _,tree=self.rendered(items)
        row=tree.xpath('//tr[@class="certificate-party"][th="Lessor"]')[0]
        self.assertIn('Lessor Director',row.text_content());self.assertIn('REP-2',row.text_content())
        self.assertNotIn('Required representative details are missing',row.text_content())

    def test_labels_routes_and_control_groups(self):
        expected=['Cover','Certificate','Core Lease','Special Conditions','Signatures','Full Lease']
        self.assertEqual([label for _,label in self.kit.COMPONENTS],expected)
        self.assertEqual([key for key,_ in self.kit.COMPONENTS],['cover','certificate','core','special_conditions','signature','lease'])
        self.app.add_url_rule('/kit/<int:property_id>.pdf',endpoint='billing_bp.lease_kit_pdf',view_func=lambda **kw:'')
        self.app.add_url_rule('/kit/<int:property_id>/email',endpoint='billing_bp.lease_kit_email',view_func=lambda **kw:'')
        with self.app.test_request_context('/'):
            rows=self.kit.action_documents(26,'saved-draft')
            for row,(key,_) in zip(rows,self.kit.COMPONENTS):self.assertIn('component='+key,row['pdf_url'])
            page=render_template('program_billing/lease_kit_view.html',title='View Lease',selected_component='core',selected_label='Core Lease',
                components=[{'key':k,'label':v,'url':'/?component='+k} for k,v in self.kit.COMPONENTS],
                states={key:{'available':True} for key,_ in self.kit.COMPONENTS},pdf_url='/protected.pdf',back_url='/',viewer_only=True,kit_view=True)
        tree=html.fromstring(page)
        self.assertEqual(tree.xpath('//*[@id="lease-kit-selectors"]/a/text()'),expected)
        self.assertEqual(tree.xpath('//*[@id="lease-kit-toolbar"]/div/@aria-label'),['Document zoom','Page navigation'])
        self.assertNotIn('target="_blank"',page)
        for action in ('play','pause','print','download','email'):self.assertNotIn('id="lease-kit-'+action+'"',page)
        css=(ROOT/'app/static/css/lease_kit.css').read_text()
        self.assertIn('justify-content:space-between',css)
        for key,_ in self.kit.COMPONENTS:self.assertIn('[data-component="'+key+'"]',css)

    def test_legacy_certificate_pdf_links_share_the_corrected_renderer(self):
        tree=ast.parse((ROOT/'app/program_billing/lease_certificate_view.py').read_text(encoding='utf-8-sig'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='pdf')
        env={'redirect':redirect,'url_for':url_for,'request':request}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'legacy-certificate-alias','exec'),env)
        self.app.add_url_rule('/kit/<int:property_id>.pdf',endpoint='billing_bp.lease_kit_pdf',view_func=lambda **kw:'')
        with self.app.test_request_context('/?draft_id=stored-draft'):
            response=env['pdf'](26)
        self.assertEqual(response.status_code,302)
        self.assertIn('component=certificate',response.location)
        self.assertIn('draft_id=stored-draft',response.location)

if __name__=='__main__':unittest.main()
