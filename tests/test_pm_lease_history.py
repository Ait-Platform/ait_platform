"""Private lease history/addenda and exact-version standardized email tests."""
import copy,io,json,sys,types,unittest
from hashlib import sha256
from zipfile import ZipFile
from unittest.mock import Mock,patch
from lxml import html
from sqlalchemy import text,event
import test_pm_property_lease_master as fixture


class LeaseHistoryTests(fixture.PropertyLeaseMasterTests):
    def url_history(self):return '/billing/leases/property/26/signed-copy?draft_id='+str(self.draft)
    def ledger(self):return self.sql('SELECT items FROM bil_lease_draft').scalar().get('_lease_history',{'documents':[],'emails':[]})
    def version(self):return str(self.sql('SELECT version FROM bil_lease_draft').scalar())
    def signed(self,kind='signed_original',**extra):
        data={'kind':kind,'lease_reference':'Synthetic Almond agreement 2024-01-01','version':self.version(),
            'signed_document':(io.BytesIO(self.docx('Signed copy synthetic')),'Signed.docx')}
        data.update(extra)
        return self.client.post(self.url_history(),data=data,content_type='multipart/form-data')
    def history_doc_url(self,identifier,suffix=''):
        return '/billing/leases/property/26/history/'+identifier+suffix+'?draft_id='+str(self.draft)
    def create_addendum(self,**extra):
        original=self.ledger()['documents'][0]
        data={'original_id':original['id'],'wording':'Synthetic clause 4 rent amendment: ZAR 1000 from 2026-11-01. All other agreed provisions continue unchanged.','version':self.version()}
        data.update(extra)
        return self.client.post('/billing/leases/property/26/addendum?draft_id='+str(self.draft),data=data,content_type='multipart/form-data')
    def mail_link(self,view):
        tree=html.fromstring(view.get_data(as_text=True))
        return tree.xpath('//*[@id="lease-header-actions"]//a[text()="Email"]')[0].get('href')
    def sender(self,result=True):
        module=types.ModuleType('app.utils.mailer');module.send_standard_report_email=Mock(return_value=result)
        return module,patch.dict(sys.modules,{'app.utils.mailer':module})

    def test_signed_versions_history_and_authorised_retrieval_preserve_particulars(self):
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(self.signed().status_code,302);first=self.ledger()['documents'][0]
        self.assertEqual(self.signed().status_code,302);second=self.ledger()['documents'][1]
        self.assertEqual((first['version'],second['version']),(1,2));self.assertNotEqual(first['id'],second['id'])
        for entry in (first,second):
            response=self.client.get(self.history_doc_url(entry['id'],'/download'));self.assertEqual(response.status_code,200)
            self.assertEqual(sha256(response.data).hexdigest(),entry['sha256']);self.assertEqual(entry['signature_status'],'not-verified')
        page=self.client.get(self.url_history());self.assertEqual(page.status_code,200)
        self.assertIn('signatures/legal validity not verified',page.get_data(as_text=True))
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual({k:v for k,v in after.items() if k!='_lease_history'},before)
        self.assertLess(page.get_data(as_text=True).index(first['uploaded_at']),page.get_data(as_text=True).index(second['uploaded_at']))
        self.sign_in(8);self.assertEqual(self.client.get(self.history_doc_url(first['id'],'/download')).status_code,404)

    def test_addendum_uses_original_snapshot_and_exact_original_id_without_full_terms(self):
        self.assertEqual(self.upload(self.cover_master('Terms not reproduced')).status_code,302)
        self.assertEqual(self.signed().status_code,302);original=self.ledger()['documents'][0]
        before=copy.deepcopy(original)
        response=self.create_addendum();self.assertEqual(response.status_code,302)
        generated=self.ledger()['documents'][-1]
        self.assertEqual(generated['kind'],'addendum_draft');self.assertEqual(generated['original_id'],original['id'])
        self.assertEqual(generated['original_sha256'],original['sha256']);self.assertEqual(self.ledger()['documents'][0],before)
        view=self.client.get(response.location);self.assertEqual(view.status_code,200)
        content=html.fromstring(view.get_data(as_text=True)).xpath('//article')[0].text_content()
        for value in ('Addendum No. 1 to Agreement of Lease','2026-11-01','ZAR 1000','Amendment Content','Signatures',original['id']):self.assertIn(value,content)
        self.assertNotIn('Terms not reproduced',content);self.assertNotIn('Introduction',content)
        self.assertEqual(self.client.get(self.history_doc_url(generated['id'],'/download')).status_code,200)

    def test_addendum_reads_original_particulars_snapshot_not_later_draft_edits(self):
        self.assertEqual(self.upload().status_code,302)
        items={'_contracting_lessors':{'identities':[{'name':'Original Contracting Landlord','identity_number':'TEST-OWNER'}]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.signed().status_code,302)
        items=self.sql('SELECT items FROM bil_lease_draft').scalar()
        items['_contracting_lessors']['identities'][0]['name']='Later Draft Owner'
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()
        response=self.create_addendum();self.assertEqual(response.status_code,302)
        content=self.client.get(response.location).get_data(as_text=True)
        self.assertIn('Original Contracting Landlord',content);self.assertNotIn('Later Draft Owner',content)

    def test_failed_sender_exception_and_stale_upload_do_not_claim_success(self):
        self.assertEqual(self.signed().status_code,302)
        count=len(self.ledger()['documents'])
        self.assertEqual(self.signed(version='0').status_code,409)
        self.assertEqual(len(self.ledger()['documents']),count)
        link=self.mail_link(self.client.get(self.history_doc_url(self.ledger()['documents'][0]['id'])))
        module,guard=self.sender();module.send_standard_report_email.side_effect=RuntimeError('Synthetic failure')
        with guard:self.assertEqual(self.client.post(link,data={'recipient':'attorney@example.com'}).status_code,400)
        self.assertEqual(self.ledger()['emails'][0]['status'],'failed')

    def test_addendum_wording_upload_is_preserved_and_signed_addendum_and_termination_link_original(self):
        self.assertEqual(self.upload().status_code,302);self.assertEqual(self.signed().status_code,302)
        original=self.ledger()['documents'][0]
        response=self.create_addendum(wording='',wording_file=(io.BytesIO(self.docx('Uploaded amendment wording')),'Wording.docx'))
        self.assertEqual(response.status_code,302)
        entries=self.ledger()['documents'];self.assertEqual([e['kind'] for e in entries],['signed_original','amendment_wording','addendum_draft'])
        self.assertIn('Uploaded amendment wording',self.client.get(response.location).get_data(as_text=True))
        for kind in ('signed_addendum','signed_termination'):
            self.assertEqual(self.signed(kind,original_id=original['id']).status_code,302)
            self.assertEqual(self.ledger()['documents'][-1]['original_id'],original['id'])
            self.assertEqual(self.ledger()['documents'][-1]['signature_status'],'not-verified')

    def test_failed_private_upload_and_invalid_addendum_do_not_publish_or_modify_original(self):
        self.assertEqual(self.upload().status_code,302);self.assertEqual(self.signed().status_code,302)
        before=copy.deepcopy(self.ledger()['documents'][0])
        self.assertEqual(self.create_addendum(wording='').status_code,400)
        self.assertEqual(self.create_addendum(original_id='invalid').status_code,404)
        self.assertEqual(self.create_addendum(wording='   ').status_code,400)
        self.assertEqual(len(self.ledger()['documents']),1)
        self.deny=True;self.assertEqual(self.signed().status_code,503);self.deny=False
        failed=self.ledger()['documents'][-1];self.assertEqual(failed['state'],'pending')
        self.assertEqual(self.client.get(self.history_doc_url(failed['id'],'/download')).status_code,404)
        self.assertEqual(self.ledger()['documents'][0],before)
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=False
        self.assertEqual(self.signed().status_code,400)

    def test_storage_verification_cannot_publish_a_record_that_changed_state(self):
        original=self.storage.upload
        def change_pending(*args):
            result=original(*args)
            self.sql("UPDATE bil_lease_document SET state='failed' WHERE state='pending'");self.session.commit()
            return result
        with patch.object(self.storage,'upload',side_effect=change_pending):
            self.assertEqual(self.signed().status_code,503)
        self.assertEqual(self.ledger()['documents'][0]['state'],'pending')
        self.assertEqual(self.sql('SELECT state FROM bil_lease_document').scalar(),'failed')
        self.assertEqual(self.client.get(self.history_doc_url(self.ledger()['documents'][0]['id'],'/download')).status_code,404)

    def test_email_form_no_send_until_explicit_post_and_exact_document_bytes(self):
        self.assertEqual(self.signed().status_code,302);entry=self.ledger()['documents'][0]
        view=self.client.get(self.history_doc_url(entry['id']));link=self.mail_link(view)
        expected=self.client.get(self.history_doc_url(entry['id'],'/download')).data
        module,guard=self.sender()
        with guard:
            form=self.client.get(link);self.assertEqual(form.status_code,200);module.send_standard_report_email.assert_not_called()
            tree=html.fromstring(form.get_data(as_text=True));token=tree.xpath('//input[@name="document_token"]/@value')[0]
            self.assertEqual(tree.xpath('//input[@name="recipient"]/@value'),['manager7@example.com'])
            response=self.client.post(link,data={'document_token':token,'recipient':'attorney@example.com','message':'Please review.'})
            self.assertEqual(response.status_code,302)
            kwargs=module.send_standard_report_email.call_args.kwargs
            self.assertEqual(kwargs['pdf_bytes'],expected);self.assertEqual(kwargs['attachment_mime'],'application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            self.assertEqual(kwargs['message'],'Please review.');self.assertEqual(kwargs['to_email'],'attorney@example.com')
            self.assertEqual(self.ledger()['emails'][0]['status'],'submitted')
            self.assertEqual(self.client.post(link,data={'recipient':'attorney@example.com'}).status_code,409)
            self.assertEqual(module.send_standard_report_email.call_count,1)

    def test_email_failure_recorded_validation_tampering_and_foreign_access_rejected(self):
        self.assertEqual(self.signed().status_code,302);entry=self.ledger()['documents'][0]
        link=self.mail_link(self.client.get(self.history_doc_url(entry['id'])))
        module,guard=self.sender(False)
        with guard:
            self.assertEqual(self.client.post(link,data={'recipient':'not-an-email'}).status_code,400)
            module.send_standard_report_email.assert_not_called()
            response=self.client.post(link,data={'recipient':'attorney@example.com'})
            self.assertEqual(response.status_code,400);self.assertIn('did not confirm',response.get_data(as_text=True))
        self.assertEqual(self.ledger()['emails'][0]['status'],'failed')
        self.assertEqual(self.client.get('/billing/leases/property/26/email?draft_id='+str(self.draft)+'&document_token=invalid').status_code,400)
        self.sign_in(8);self.assertEqual(self.client.get(link).status_code,404)

    def test_working_view_token_pins_master_and_rejects_changed_saved_particulars(self):
        self.assertEqual(self.upload(self.cover_master('Version one')).status_code,302)
        view=self.property_view();self.assertEqual(view.status_code,200);link=self.mail_link(view)
        self.assertEqual(self.upload(self.cover_master('Version two')).status_code,302)
        module,guard=self.sender()
        with guard:
            self.assertEqual(self.client.post(link,data={'recipient':'attorney@example.com'}).status_code,302)
            from app.program_billing import lease_preview as preview
            content='\n'.join(preview.block_text(b) for b in preview.extract_blocks(module.send_standard_report_email.call_args.kwargs['pdf_bytes']))
            self.assertIn('Version one',content);self.assertNotIn('Version two',content)
        new_link=self.mail_link(self.property_view())
        self.sql('UPDATE bil_lease_draft SET version=version+1');self.session.commit()
        module,guard=self.sender()
        with guard:
            self.assertEqual(self.client.post(new_link,data={'recipient':'attorney@example.com'}).status_code,409)
            module.send_standard_report_email.assert_not_called()

    def test_view_toolbar_print_all_pages_and_no_writes(self):
        self.assertEqual(self.upload(self.cover_master()).status_code,302)
        statements=[]
        def capture(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:response=self.property_view()
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True));header=tree.xpath('//*[@id="lease-header-actions"]')[0]
        self.assertIn('flex-col',header.get('class'))
        self.assertEqual(header.xpath('./div/a/text()'),['Email']);self.assertEqual(header.xpath('./div/button/text()'),['Print'])
        self.assertEqual(header.xpath('./div/button/@onclick'),['window.print()'])
        content=response.get_data(as_text=True);self.assertIn('[data-lease-page][hidden] { display: block !important',content)
        self.assertIn('#lease-header-actions, #lease-page-controls, script { display: none !important;',content)
        self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for s in statements))


class StandardizedMailerCompatibilityTests(unittest.TestCase):
    def test_existing_report_defaults_and_document_attachment_mode(self):
        import ast
        source=(fixture.ROOT/'app/utils/mailer.py').read_text(encoding='utf-8')
        root=ast.parse(source);node=next(n for n in root.body if isinstance(n,ast.FunctionDef) and n.name=='send_standard_report_email')
        class Message:
            def __init__(self,**kwargs):self.kwargs=kwargs;self.attachments=[]
            def attach(self,*args):self.attachments.append(args)
        mail=Mock();namespace={'Message':Message,'mail':mail}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'standard-mailer','exec'),namespace)
        sender=namespace['send_standard_report_email']
        self.assertIs(sender('user@example.com','Report','https://example.test/report',b'%PDF-test'),True)
        original=mail.send.call_args.args[0]
        self.assertIn('assessment report',original.kwargs['body']);self.assertEqual(original.attachments[0][1],'application/pdf')
        self.assertIs(sender('attorney@example.com','Lease','',b'DOCX',filename='Lease.docx',attachment_mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',message='Review this.',document_label='lease document'),True)
        document=mail.send.call_args.args[0]
        self.assertIn('Review this.',document.kwargs['body']);self.assertEqual(document.attachments[0][1],'application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        self.assertEqual(document.attachments[0][2],b'DOCX')
        mail.send.side_effect=RuntimeError('Synthetic sender failure')
        with self.assertRaises(RuntimeError):sender('user@example.com','Lease','',b'DOCX')
