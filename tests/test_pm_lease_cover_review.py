"""Essential cover privacy/identity and additive banking-migration review."""
import ast,io,unittest
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile
from lxml import etree
import test_pm_lease_premises_simple as fixture
import test_pm_property_lease_master as masters

class LeaseCoverReviewTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.SimplifiedPremisesSmokeTest.setUp
    sign_in=fixture.SimplifiedPremisesSmokeTest.sign_in
    sql=fixture.SimplifiedPremisesSmokeTest.sql
    payload=fixture.SimplifiedPremisesSmokeTest.payload
    assign=fixture.SimplifiedPremisesSmokeTest.assign
    create=fixture.SimplifiedPremisesSmokeTest.create
    def test_cover_names_certificate_identity_and_additive_migration(self):
        from app.program_billing import lease_standard as standard,lease_preview as preview
        items={'_contracting_lessors':{'identities':[{'type':'individual','name':'sunjithlall nanhoo','identity_number':'PRIVATE-OWNER-1'},{'type':'individual','name':'Alexandra Jane Example','identity_number':'PRIVATE-OWNER-2'},{'type':'company','name':'Dale Holdings (Pty) Ltd','identity_number':'PRIVATE-REG-1'}]},'_parties':{'lessees':[{'party_type':'juristic_person','legal_name':'Community Services NPC','identity_number':'PRIVATE-REG-2'},{'party_type':'natural_person','legal_name':'Brian Charles Smith','identity_number':'PRIVATE-TENANT-1'}]}}
        names=standard.cover_names(items)
        self.assertEqual(names['landlords'],'S. Nanhoo; A. Example; Dale Holdings (Pty) Ltd')
        self.assertEqual(names['tenants'],'Community Services NPC; B. Smith')
        source=masters.PropertyLeaseMasterTests.docx(self,'1. Reference clause remains unchanged.')
        record={'sha256':sha256(source).hexdigest(),'version':1}
        state={'terms_version':'development','applicability':{'terms_sha256':record['sha256'],'excluded':[],'clause_inventory_sha256':standard.digest(standard.inventory(source))}}
        merged=standard.build_document(items,state,record,source)
        blocks=preview.extract_blocks(merged);break_at=next(i for i,b in enumerate(blocks) if b['kind']=='page_break')
        cover='\n'.join(preview.block_text(b) for b in blocks[:break_at])
        for text in ('S. Nanhoo','A. Example','Dale Holdings (Pty) Ltd','Community Services NPC','B. Smith'):self.assertIn(text,cover)
        for text in ('PRIVATE-','Landlord','Tenant','Capacity','Address'):self.assertNotIn(text,cover)
        certificate='\n'.join(preview.block_text(b) for b in blocks[break_at+1:])
        for text in ('sunjithlall nanhoo','Alexandra Jane Example','Brian Charles Smith','PRIVATE-OWNER-1','PRIVATE-REG-2'):self.assertIn(text,certificate)
        self.assertIn('1. Reference clause remains unchanged.',certificate)
        tree=ast.parse(Path('migrations/versions/pm_lease_payments_001.py').read_text(encoding='utf-8-sig'))
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id=='op']
        self.assertEqual([n.func.attr for n in calls],['get_bind','create_table','create_table'])
        self.assertEqual({n.args[0].value for n in calls if n.func.attr=='create_table'},{'bil_property_payment_account','bil_property_utility_arrangement'})
