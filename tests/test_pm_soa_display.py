"""PM ledger presentation: effective dates, tile flashes and latest-five window."""
import unittest
from datetime import datetime, timezone
from pathlib import Path
from lxml import html
from sqlalchemy import text
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
from flask import flash, redirect
import test_pm_soa_ledger as fixture


class PMDisplayTests(unittest.TestCase):
    setUp=fixture.PMTenantLedgerTests.setUp
    path=fixture.PMTenantLedgerTests.path
    def test_latest_five_expand_in_fives_with_full_running_balances(self):
        for n in range(1,13):
            self.session.execute(text("""INSERT INTO bil_tenant_ledger
                (tenant_id,txn_date,description,kind,amount,ref)
                VALUES(11,:date,'Rent Due','charge',:amount,:ref)"""),
                dict(date='2026-10-'+str(n).zfill(2),amount=n,ref='D'+str(n).zfill(4)))
        self.session.commit()
        page=self.client.get(self.path(),headers=self.headers)
        tree=html.fromstring(page.data)
        rows=tree.xpath('//*[@id="ledger-transactions"]/tbody/tr')
        self.assertEqual([r[0].text_content() for r in rows],['2026-10-'+str(n).zfill(2) for n in range(8,13)])
        self.assertEqual([r[4].text_content() for r in rows],['36.00','45.00','55.00','66.00','78.00'])
        self.assertEqual(tree.xpath('string(//*[@id="ledger-summary"]/tbody/tr/td[4])'),'R 78.00')
        more=tree.xpath('//a[starts-with(text(),"Show More (")]/@href')[0]
        expanded=self.client.get(more,headers=self.headers);expanded_tree=html.fromstring(expanded.data)
        self.assertEqual(len(expanded_tree.xpath('//*[@id="ledger-transactions"]/tbody/tr')),10)
        less=expanded_tree.xpath('//a[starts-with(text(),"Show Less (")]/@href')[0]
        self.assertEqual(len(html.fromstring(self.client.get(less,headers=self.headers).data).xpath('//*[@id="ledger-transactions"]/tbody/tr')),5)
    def test_calendar_dates_do_not_reinterpret_naive_effective_dates_as_utc(self):
        from app.program_billing.lease_soa_dates import effective_date
        self.assertEqual(effective_date(datetime(2026,10,24,0,0)).isoformat(),'2026-10-24')
        self.assertEqual(effective_date(datetime(2026,10,23,23,30,tzinfo=timezone.utc)).isoformat(),'2026-10-24')
        self.assertEqual(effective_date(datetime(2026,10,24,22,30,tzinfo=timezone.utc)).isoformat(),'2026-10-25')
    def test_success_flash_is_inside_tile_using_shared_style_once(self):
        root=Path(__file__).resolve().parents[1]
        parent=(root/'templates/layout.html').read_text(encoding='utf-8')
        start=parent.index('{% block flashes %}')
        end=parent.index('{% endblock %}',parent.index('{% endblock %}',start)+1)+len('{% endblock %}')
        self.app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':parent[start:end]+'{% block content %}{% endblock %}'}),FileSystemLoader(root/'templates')])
        self.app._got_first_request=False
        def notify():
            flash('Correction saved: reversal and replacement recorded.','success')
            return redirect(self.path())
        self.app.add_url_rule('/display-test/notify',view_func=notify)
        response=self.client.get('/display-test/notify',headers=self.headers,follow_redirects=True)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        messages=tree.xpath('//*[@id="flash-toast-container"]')
        self.assertEqual(len(messages),1)
        self.assertTrue(messages[0].xpath('ancestor::*[@id="pm-tenant-ledger"]'))
        self.assertTrue(messages[0].xpath('.//*[contains(@class,"text-emerald-800")]'))

    def test_ledger_actions_replace_navigation_once_in_exact_order(self):
        expected=['Charges','Payment','Fixed Charges','Opening Balance','Letterhead','SOA']
        page=self.client.get(self.path(),headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        rows=tree.xpath('//*[@id="ledger-actions"]');self.assertEqual(len(rows),1)
        actions=rows[0].xpath('./a|./button')
        self.assertEqual([a.text_content() for a in actions],expected)
        for style in ('justify-end','flex-wrap','gap-2','text-sm'):
            self.assertIn(style,rows[0].get('class'))
        for action in actions:
            self.assertIn('px-3',action.get('class'))
            self.assertIn('whitespace-nowrap',action.get('class'))
        for old_label in ('Post Charge','Record Payment','SOA Branding','Tenant Statement'):
            self.assertFalse(tree.xpath('//a[text()="'+old_label+'"]'))
        self.assertEqual(rows[0].getprevious().xpath('string(.//h1)'),'Tenant Ledger')
        self.assertTrue(rows[0].getnext().xpath('.//*[@id="ledger-summary"]'))
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]'))
        for label in expected:self.assertEqual(len(tree.xpath('//a[text()="'+label+'"]|//button[text()="'+label+'"]')),1)
        self.assertEqual([a.get('href') for a in actions],
            [self.path()+'?posting=charge',self.path()+'?posting=payment',self.path()+'/fixed',self.path()+'?posting=opening_balance','/manager/tenant_accounts/26/branding','/manager/tenant_accounts/26/tenant/11/statement'])
        # Opening Balance retains its once-per-tenant disabled state in fourth position.
        self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,description,kind,amount,ref) VALUES(11,'Opening Balance','opening_balance',0,'D0001')"));self.session.commit()
        after=html.fromstring(self.client.get(self.path(),headers=self.headers).data)
        actions=after.xpath('//*[@id="ledger-actions"]/*')
        self.assertEqual([a.text_content() for a in actions],expected)
        self.assertIsNotNone(actions[3].get('disabled'))
        # The same shared PM navigation remains on the tenant-selection page.
        other=html.fromstring(self.client.get('/manager/tenant_accounts?property_id=26',headers=self.headers).data)
        self.assertEqual(other.xpath('//nav[@aria-label="Property Management"]/a/text()'),
            ['Property & Owner','Lease','METSOA','Property Portfolio'])

    def test_exact_displayed_total_counts_and_distinct_colours(self):
        for n in range(20):
            self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,txn_date,description,kind,amount) VALUES(11,'2026-10-01','Historical Rent Due','charge',1)"))
        self.session.commit()
        for count in (5,10,15,20):
            page=self.client.get(self.path()+'?visible='+str(count),headers=self.headers)
            tree=html.fromstring(page.data)
            more=tree.xpath('//a[starts-with(text(),"Show More (")]')
            less=tree.xpath('//a[starts-with(text(),"Show Less (")]')
            if count<20:
                self.assertEqual(more[0].text_content(),'Show More ('+str(count)+' of 20)')
                self.assertIn('bg-blue-100',more[0].get('class'))
            else:self.assertFalse(more)
            if count>5:
                self.assertEqual(less[0].text_content(),'Show Less ('+str(count)+' of 20)')
                self.assertIn('bg-amber-100',less[0].get('class'))
    def test_manual_dropdown_excludes_scheduled_descriptions_but_keeps_history(self):
        page=self.client.get(self.path()+'?posting=charge',headers=self.headers)
        options=html.fromstring(page.data).xpath('//select[@name="description"]/option/text()')
        self.assertEqual(options,['Select description','Recoverable Repairs','Late Payment Charges','Other Charge'])
