"""Stage G: isolated DOCX merger safety checks; no generation endpoint."""
import importlib.util
import hashlib
import io
import unittest
from zipfile import ZipFile
from lxml import etree as ET
import test_pm_property_management as properties

class MergeStageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('isolated_lease_merge',properties.ROOT/'app/program_billing/lease_merge.py')
        cls.merge=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.merge)

    def master(self,body=None):
        stream=io.BytesIO()
        xml=body or '<w:p><w:r><w:rPr><w:b/></w:rPr><w:t>[NA</w:t></w:r><w:r><w:t>ME] on [DATE]</w:t></w:r></w:p>'
        with ZipFile(stream,'w') as archive:
            archive.writestr('[Content_Types].xml','<Types/>')
            archive.writestr('word/document.xml','<w:document xmlns:w="'+self.merge.W+'" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml" mc:Ignorable="w14"><w:body>'+xml+'</w:body></w:document>')
            archive.writestr('word/header1.xml','<w:hdr xmlns:w="'+self.merge.W+'"><w:p><w:r><w:t>[NAME]</w:t></w:r></w:p></w:hdr>')
        return stream.getvalue()

    def test_split_runs_headers_snapshot_and_master_preservation(self):
        master=self.master();digest=hashlib.sha256(master).hexdigest();values={'[NAME]':'Tenant & Company','[DATE]':'2026-10-15'}
        result=self.merge.merge_draft_docx(master,values,approved_sha256=digest)
        self.assertEqual(hashlib.sha256(master).hexdigest(),digest)
        self.assertEqual(result['master_sha256'],digest);self.assertEqual(result['merged_values'],values)
        self.assertNotEqual(result['docx'],master);self.assertEqual(result['status'],'draft-not-approved')
        with ZipFile(io.BytesIO(result['docx'])) as archive:
            root=ET.fromstring(archive.read('word/document.xml'))
            self.assertIn('Tenant & Company on 2026-10-15',''.join(root.itertext()))
            self.assertIsNotNone(root.find('.//{'+self.merge.W+'}b'))
            self.assertIn('w14',root.nsmap)
            self.assertIn('Tenant & Company',''.join(ET.fromstring(archive.read('word/header1.xml')).itertext()))

    def test_fail_closed_missing_unknown_unapproved_or_unresolved_mapping(self):
        master=self.master();digest=hashlib.sha256(master).hexdigest()
        for values,approved in (({'[NAME]':'Tenant'},digest),({'[NAME]':'Tenant','[DATE]':'Date','[OTHER]':'X'},digest),({'[NAME]':'[UNRESOLVED]','[DATE]':'Date'},digest),({'[NAME]':'Tenant','[DATE]':'Date'},'wrong')):
            with self.assertRaises(ValueError):self.merge.merge_draft_docx(master,values,approved_sha256=approved)
        unsupported=self.master('<w:p><w:r><w:instrText> MERGEFIELD NAME </w:instrText></w:r></w:p>')
        with self.assertRaises(ValueError):self.merge.merge_draft_docx(unsupported,{},approved_sha256=hashlib.sha256(unsupported).hexdigest())
