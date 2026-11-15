"""Sample pack contents, page selection and absence of account side effects."""
from decimal import Decimal
import unittest
import fitz
import test_protrade_shop_register as base
from app.protrade_samples import SAMPLES, sample_documents


class MarketingSampleTests(unittest.TestCase):
    setUp=base.ShopRegisterTests.setUp

    def test_all_trade_documents_and_balances_are_readable_in_pdf(self):
        documents=sample_documents()
        with fitz.open(base.ROOT/'app/static/pdf/protrade-samples.pdf') as pdf:
            self.assertEqual(len(pdf),len(SAMPLES)*2)
            for index,sample in enumerate(documents):
                page=pdf[index]
                text=page.get_text()
                self.assertIn(sample['issuer']['name'],text)
                self.assertIn(sample['document']['title'],text)
                self.assertIn(sample['document']['totals'][0][1],text)
                self.assertIn('Fictional business and client',text)
                for word in page.get_text('words'):
                    self.assertGreaterEqual(word[0],0)
                    self.assertLessEqual(word[2],page.rect.width)
                    self.assertLessEqual(word[3],page.rect.height)
            self.assertIn('Example Patient',pdf[14].get_text())
            self.assertIn('Amount Due (R)',pdf[15].get_text())
            self.assertIn('445.00',pdf[15].get_text())

    def test_gallery_selects_medical_statement_without_charges_or_records(self):
        balance=base.models.AitTokenWallet.query.one().balance
        response=self.client.get('/mechanic/samples?trade=crm_health&kind=statement')
        self.assertEqual(response.status_code,200)
        self.assertIn('protrade-samples.pdf#page=16',response.get_data(as_text=True))
        self.assertEqual(self.client.get('/mechanic/samples?trade=unknown').status_code,404)
        self.assertEqual(base.models.AitTokenWallet.query.one().balance,balance)
        self.assertEqual(base.TokenTransaction.query.count(),0)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),0)
        self.assertEqual(base.DebtorLedger.query.count(),0)


if __name__=='__main__':unittest.main()
