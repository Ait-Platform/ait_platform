"""Private PM branding on isolated PostgreSQL; R2 transport is in-memory in tests."""
import importlib.util
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from lxml import html
from PIL import Image
import test_pm_soa_ledger as fixture


def apply_branding(connection):
    path=Path(__file__).resolve().parents[1]/'migrations/versions/pm_soa_brand_001.py'
    spec=importlib.util.spec_from_file_location('branding_migration',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with patch.object(module,'op',Operations(MigrationContext.configure(connection))):module.upgrade()


class SOABrandingTests(unittest.TestCase):
    path=fixture.PMTenantLedgerTests.path
    def setUp(self):
        fixture.PMTenantLedgerTests.setUp(self)
        apply_branding(self.conn);self.conn.commit()
        from app.program_billing import lease_soa_branding
        self.brand=lease_soa_branding;self.objects={}
        def upload(key,data,mime):self.objects[key]=data;return key
        for target,name,value in [(self.brand.storage,'upload',upload),(self.brand.storage,'destination',lambda:'private-test'),(self.brand.storage.r2,'read_file_from_r2',lambda key,**kw:self.objects[key])]:
            mock=patch.object(target,name,side_effect=value);mock.start();self.addCleanup(mock.stop)
    def hub(self,p=26):return '/manager/tenant_accounts/'+str(p)+'/branding'
    def form_data(self,**changes):
        page=self.client.get(self.hub(),headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        key=html.fromstring(page.data).xpath('//input[@name="form_key"]/@value')[0]
        data=dict(csrf_token=self.csrf,form_key=key,action='create',kind='letterhead',title='My letterhead',business_name='Property Manager',address='Durban',contact='example@example.com')
        data.update(changes);return data
    def post_brand(self,data,p=26):return self.client.post(self.hub(p),data=data,headers=self.headers)
    def test_create_preview_select_and_ledger_header_without_financial_changes(self):
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all()
        data=self.form_data(business_name='<script>alert(1)</script>');self.assertEqual(self.post_brand(data).status_code,302)
        self.assertEqual(self.post_brand(data).status_code,302)
        row=self.session.execute(text('SELECT * FROM bil_pm_soa_branding')).mappings().one()
        page=self.client.get(self.hub(),headers=self.headers)
        self.assertIn('&lt;script&gt;',page.text);self.assertNotIn('<script>alert(1)',page.text)
        self.assertEqual(self.post_brand(dict(csrf_token=self.csrf,action='select',kind='letterhead',id=str(row['id']))).status_code,302)
        ledger=self.client.get(self.path(),headers=self.headers)
        tree=html.fromstring(ledger.data)
        self.assertTrue(tree.xpath('//header[contains(@class,"soa-branding")]'))
        self.assertLess(ledger.text.index('soa-branding'),ledger.text.index('No transactions recorded'))
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all(),before)
    def test_logo_upload_is_private_sanitized_and_preview_authorised(self):
        out=BytesIO();Image.new('RGB',(30,20),'blue').save(out,format='PNG')
        data=self.form_data(action='upload',kind='logo');data['branding_file']=(BytesIO(out.getvalue()),'logo.png')
        response=self.post_brand(data);self.assertEqual(response.status_code,302,response.text)
        row=self.session.execute(text('SELECT * FROM bil_pm_soa_branding')).mappings().one()
        self.assertEqual(len(self.objects),2)
        self.assertTrue(all(key.startswith('private/pm-soa/manager-7/property-26/') for key in self.objects))
        url=self.hub()+'/'+str(row['id'])+'/preview'
        response=self.client.get(url,headers=self.headers)
        self.assertEqual(response.status_code,200);self.assertEqual(response.mimetype,'image/png')
        self.assertIn('private',response.headers['Cache-Control'])
        self.assertEqual(self.client.get(url).status_code,403)
        self.assertEqual(self.client.get(self.hub(28)+'/'+str(row['id'])+'/preview',headers=self.headers).status_code,404)
        self.assertEqual(self.post_brand(dict(csrf_token=self.csrf,action='select',kind='logo',id=str(row['id'])),27).status_code,404)
    def test_invalid_files_csrf_and_storage_failure_preserve_selection(self):
        for name,payload in [('logo.svg',b'<svg/>'),('logo.png',b'<script>bad</script>'),('big.png',b'x'*(5*1024*1024+1))]:
            data=self.form_data(action='upload',kind='logo');data['branding_file']=(BytesIO(payload),name)
            self.assertEqual(self.post_brand(data).status_code,400)
        self.assertEqual(self.post_brand(self.form_data(csrf_token='bad')).status_code,400)
        self.assertEqual(self.client.get(self.hub(28),headers=self.headers).status_code,404)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_pm_soa_branding')).scalar(),0)
        out=BytesIO();Image.new('RGB',(20,20)).save(out,format='PNG')
        data=self.form_data(action='upload',kind='logo');data['branding_file']=(BytesIO(out.getvalue()),'logo.png')
        with patch.object(self.brand.storage,'upload',side_effect=ValueError('Private storage unavailable')):
            self.assertEqual(self.post_brand(data).status_code,400)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_pm_soa_branding')).scalar(),0)
    def test_missing_branding_migration_does_not_break_ledger(self):
        self.session.execute(text('DROP TABLE bil_pm_soa_branding'));self.session.commit()
        self.assertEqual(self.client.get(self.path(),headers=self.headers).status_code,200)
        page=self.client.get(self.hub(),headers=self.headers)
        self.assertEqual(page.status_code,200);self.assertIn('pm_soa_brand_001',page.text)
        self.assertFalse(html.fromstring(page.data).xpath('//form'))
    def test_pdf_letterhead_preview_and_type_checks(self):
        import fitz
        document=fitz.open();document.new_page().insert_text((50,50),'Test letterhead');payload=document.tobytes();document.close()
        data=self.form_data(action='upload');data['branding_file']=(BytesIO(payload),'letterhead.pdf')
        self.assertEqual(self.post_brand(data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_pm_soa_branding')).scalar(),1)
        data=self.form_data(action='upload',kind='logo');data['branding_file']=(BytesIO(payload),'logo.pdf')
        self.assertEqual(self.post_brand(data).status_code,400)
