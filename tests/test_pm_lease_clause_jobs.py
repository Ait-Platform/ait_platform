"""Guard against accidentally restoring the retired PDF background loop."""
import unittest
from pathlib import Path
class RetiredBackgroundIndexTests(unittest.TestCase):
    def test_no_executor_polling_or_pdf_processing(self):
        root=Path(__file__).resolve().parents[1]
        jobs=(root/'app/program_billing/lease_clause_jobs.py').read_text()
        client=(root/'app/static/js/lease_addendum_pdf_search.js').read_text()
        index=(root/'app/program_billing/lease_clause_index.py').read_text()
        self.assertNotIn('ThreadPoolExecutor',jobs)
        self.assertNotIn('setTimeout',client)
        self.assertNotIn('build_pdf',index)
        self.assertNotIn('fitz',index)
