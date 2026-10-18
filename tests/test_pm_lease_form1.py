"""Form 1 reuses saved Part A selections and the actual Stage 5 party mappings."""
import copy,json
from lxml import html
import test_pm_lease_tenant_page_selection as fixture

class Form1Tests(fixture.TenantPageSelectionTests):
    tokens=('[S1 LEGAL LANDLORD(S)]','[LANDLORD SIGNATORY NAME AND CAPACITY]','[S1 LEGAL TENANT]','[TENANT SIGNATORY NAME AND CAPACITY]')
    def panel(self):
        response=self.client.get(self.route('particulars/parties'));self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        return tree,{row.get('data-party-placeholder'):row.xpath('./dd[@data-saved-merge-value]')[0].text_content() for row in tree.xpath('//*[@id="form-1-party-particulars"]//div[@data-party-placeholder]')}

    def test_four_independent_saved_values_reopen_and_master_locations(self):
        owners,ids=self.prepare()
        # A saved tenant can resolve independently of landlord/representative details.
        self.assertEqual(self.select([ids[2]]).status_code,302)
        _,partial=self.panel()
        self.assertEqual(partial[self.tokens[0]],self.tokens[0])
        self.assertEqual(partial[self.tokens[1]],self.tokens[1])
        self.assertEqual(partial[self.tokens[2]],'Contracting Company')
        self.assertIn('Company Representative',partial[self.tokens[3]])
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        self.complete_other_parts();before=copy.deepcopy(self.stored())
        tree,values=self.panel()
        self.assertEqual(set(values),set(self.tokens))
        self.assertEqual(values,{token:self.workflow_module.party_placeholder_values(before)[token] for token in self.tokens})
        for owner in owners:
            self.assertIn(owner['name'],values[self.tokens[0]])
            self.assertIn(owner['identity_number'],values[self.tokens[0]])
        self.assertIn(owners[0]['name'],values[self.tokens[1]])
        self.assertIn(owners[0]['identity_number'],values[self.tokens[1]])
        self.assertIn('REP-ID',values[self.tokens[3]])
        self.assertIn('Capacity: Not supplied',tree.text_content())
        self.assertNotIn('duly authorised',values[self.tokens[1]])
        checked=tree.xpath('//input[@name="representative_owner_id" and @checked]/@value')
        self.assertEqual(checked,[str(owners[0]['owner_id'])])
        self.assertEqual(self.panel()[1],values)
        view=self.client.get(self.route('view'));self.assertEqual(view.status_code,200)
        # Match the actual master schedule label and its adjacent merged value cell.
        doc=html.fromstring(view.get_data(as_text=True))
        for label,token in zip(('Landlord legal identity','Landlord signatory','Tenant legal identity','Tenant signatory'),self.tokens):
            rows=doc.xpath('//article//tr[td[1][normalize-space()=$label]]',label=label)
            self.assertEqual(len(rows),1)
            actual=rows[0].xpath('./td[2]')[0].text_content().strip()
            self.assertEqual(actual,values[token])
        self.assertEqual(self.stored(),before)
        self.assertEqual(self.save_parties(owners[1]).status_code,302)
        after=self.stored()
        for key in ('_premises','_amounts','_banking_utilities','_additional_clauses'):
            self.assertEqual(after.get(key),before.get(key))
        for key in ('s1-17','s1-18','s1-19','s1-20'):
            self.assertEqual(after.get(key),before.get(key))
        self.assertEqual(after['_parties']['lessees'],before['_parties']['lessees'])
        print('FORM1_MERGED_RESULTS='+json.dumps(values))

    def test_blank_form_displays_exact_unresolved_tokens_and_authorization(self):
        self.schema();self.owners()
        before=copy.deepcopy(self.stored())
        _,values=self.panel()
        for token in self.tokens:self.assertEqual(values[token],token)
        self.assertEqual(self.stored(),before)
        self.sign_in(8)
        self.assertEqual(self.client.get(self.route('particulars/parties')).status_code,404)
