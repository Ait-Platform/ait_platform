"""Individual annual approvals on isolated PostgreSQL tariff/audit fixtures."""
from decimal import Decimal
from datetime import date
import unittest
from unittest.mock import patch
from lxml import html
from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError
from test_pm_annual_tariff_register import AnnualRegisterTests


class ActivationTests(AnnualRegisterTests):
    def seed_band(self, code, identifier=10, rate='5.45', effective='2025-07-01'):
        index=int(code[-1])-1
        self.seed(identifier,code,rate,effective,'sewerage' if code.startswith('S') else 'water','kL')
        factor=999 if code=='S04' else self.activation.WIDTHS[index]
        upper=None if code=='S04' else self.activation.UPPERS[index]
        self.conn.execute(text('UPDATE bil_tariff SET block_start=:s,block_end=:e,reduction_factor=:f WHERE id=:id'),
                          dict(s=self.activation.LOWERS[index],e=upper,f=factor,id=identifier));self.conn.commit()

    def path(self, code):return self.url+'/'+code+'/activate'

    def approval(self, code):
        response=self.client.get(self.path(code),query_string=dict(financial_year='2026/27'))
        self.assertEqual(response.status_code,200,response.text)
        fields={node.get('name'):node.get('value') for node in html.fromstring(response.data).xpath('//input[@type="hidden"]')}
        return dict(fields,confirmed='yes',unit_confirmed='R/kL')

    def submit(self, code, fields):
        response=self.client.post(self.path(code),data=fields)
        self.conn.commit()
        return response

    def test_single_sewerage_approval_preserves_history_and_audit(self):
        self.seed_band('S01')
        before=self.snapshot()
        fields=self.approval('S01')
        self.assertEqual(before,self.snapshot())
        response=self.submit('S01',dict(fields,rate='0.01',effective_date='2025-07-01',source_reference='forged'))
        self.assertEqual(response.status_code,302,response.text)
        after=self.snapshot()['bil_tariff']
        self.assertEqual(len(after),len(before['bil_tariff'])+1)
        for old in before['bil_tariff']:self.assertIn(old,after)
        versions=[row for row in after if row['code']=='S01']
        chosen,_=self.review.resolve(versions,date(2026,7,1))
        self.assertEqual(Decimal(str(chosen[0]['rate'])),Decimal('6.53'))
        older,_=self.review.resolve(versions,date(2026,6,30))
        self.assertEqual(older[0]['id'],10)
        audit=dict(self.conn.execute(text('SELECT * FROM bil_tariff_verification WHERE tariff_id=:id'),dict(id=chosen[0]['id'])).mappings().one())
        self.assertEqual(audit['verified_by'],7)
        self.assertIn('#page=284',audit['source_reference']);self.assertIn('PDF page287',audit['source_reference'])
        self.assertIn('supersedes 10',audit['source_reference'])
        self.assertIn('no individual break-pressure tank',audit['municipal_category'])

    def test_water_approval_keeps_dimensions_and_other_charges(self):
        self.seed_band('W02',rate='46.70')
        self.seed(20,'W_MGT','144.54','2025-07-01','water_fixed','fixed')
        before=self.snapshot()['bil_tariff']
        self.assertEqual(self.submit('W02',self.approval('W02')).status_code,302)
        after=self.snapshot()['bil_tariff'];new=next(row for row in after if row['code']=='W02' and row['effective_date']=='2026-07-01')
        self.assertEqual(Decimal(str(new['rate'])),Decimal('52.30'))
        old=next(row for row in before if row['code']=='W02')
        for key in ('code','utility_type','block_start','block_end','reduction_factor','unit'):self.assertEqual(old[key],new[key])
        for row in before:self.assertIn(row,after)
        self.assertEqual([row for row in before if row['code']=='W_MGT'],[row for row in after if row['code']=='W_MGT'])

    def test_missing_fifth_band_can_be_added_individually(self):
        before=self.snapshot()['bil_tariff']
        response=self.submit('S05',self.approval('S05'))
        self.assertEqual(response.status_code,302,response.text)
        after=self.snapshot()['bil_tariff'];new=next(row for row in after if row['code']=='S05')
        self.assertEqual(Decimal(str(new['rate'])),Decimal('37.60'))
        self.assertEqual(new['block_start'],1501);self.assertEqual(new['effective_date'],'2026-07-01')
        self.assertEqual(len(after),len(before)+1)

    def test_hold_and_out_of_scope_components_never_activate(self):
        before=self.snapshot()
        for code in ('W_MGT','S_REFUSE','ELEC01','W_LOSS','S_SURCHARGE'):
            self.assertEqual(self.client.get(self.path(code)).status_code,400)
        self.assertEqual(self.client.get(self.path('W01'),query_string=dict(financial_year='2025/26')).status_code,400)
        self.assertEqual(before,self.snapshot())

    def test_missing_source_or_changed_candidate_blocks_confirmation_and_save(self):
        self.seed_band('S01')
        fields=self.approval('S01');before=self.snapshot()
        original=self.activation.candidates('2026/27')
        changed=[dict(row,**{'Source / PDF page':''}) if row['component']=='S01' else row for row in original]
        with patch.object(self.activation,'candidates',return_value=changed):
            self.assertEqual(self.client.get(self.path('S01')).status_code,400)
            self.assertEqual(self.submit('S01',fields).status_code,409)
        self.assertEqual(before,self.snapshot())

    def test_duplicate_date_and_replayed_form_cannot_insert_twice(self):
        self.seed_band('S01');fields=self.approval('S01')
        self.assertEqual(self.submit('S01',fields).status_code,302)
        before=self.snapshot()
        self.assertEqual(self.submit('S01',fields).status_code,409)
        self.assertEqual(self.client.get(self.path('S01')).status_code,400)
        self.assertEqual(before,self.snapshot())

    def test_stale_form_rejects_concurrent_rate_or_evidence_changes(self):
        self.seed_band('S01');fields=self.approval('S01')
        self.conn.execute(text('UPDATE bil_tariff SET rate=6 WHERE id=10'));self.conn.commit()
        before=self.snapshot();response=self.submit('S01',fields)
        self.assertEqual(response.status_code,409);self.assertIn('changed',response.text)
        self.assertEqual(before,self.snapshot())

    def test_confirmation_csrf_actor_and_expiry_are_required(self):
        self.seed_band('S01');fields=self.approval('S01');before=self.snapshot()
        self.assertEqual(self.client.post(self.path('S01'),data=dict(fields,csrf_token='bad')).status_code,400)
        self.assertEqual(self.submit('S01',dict(fields,confirmed='')).status_code,409)
        self.assertEqual(self.submit('S01',dict(fields,approval_token='invalid')).status_code,409)
        with self.app.app_context():
            fake=self.activation.serializer().dumps(dict(actor=999,year='2026/27',code='S01',digest='test'))
        self.assertEqual(self.submit('S01',dict(fields,approval_token=fake)).status_code,403)
        with self.app.app_context():
            payload=self.activation.serializer().loads(fields['approval_token'])
            with patch('itsdangerous.timed.TimestampSigner.get_timestamp',return_value=1):
                expired=dict(fields,approval_token=self.activation.serializer().dumps(payload))
        self.assertEqual(self.submit('S01',expired).status_code,409)
        self.assertEqual(before,self.snapshot())

    def test_two_concurrent_approvals_insert_one_version_and_one_audit(self):
        import ast,re,types,uuid
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from sqlalchemy.orm import Session
        from test_pm_admin_landing import ROOT
        table='public.test_annual_'+uuid.uuid4().hex
        audit_table=table+'_verification'
        self.assertRegex(table,r'^public\.test_annual_[a-f0-9]{32}$')
        self.conn.execute(text('CREATE TABLE '+table+' (LIKE public.bil_tariff INCLUDING CONSTRAINTS)'))
        self.conn.execute(text('ALTER TABLE '+table+' ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY'))
        self.conn.execute(text('CREATE TABLE '+audit_table+' (LIKE bil_tariff_verification INCLUDING ALL)'))
        self.conn.execute(text("INSERT INTO "+table+"(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) VALUES('sewerage','S01','S01',5.45,0,200,0.2,'kL','2025-07-01')"))
        self.conn.commit()
        def cleanup():
            self.conn.rollback();self.conn.execute(text('DROP TABLE '+audit_table));self.conn.execute(text('DROP TABLE '+table));self.conn.commit()
        self.addCleanup(cleanup)
        def isolated_tree(path):
            tree=ast.parse((ROOT/path).read_text(encoding='utf8'))
            tree.body=[n for n in tree.body if not (isinstance(n,ast.ImportFrom) and (n.module or '').startswith('app.'))]
            return tree
        service_tree=isolated_tree('app/admin/programs/billing/tariff_management.py')
        approval_tree=isolated_tree('app/admin/programs/billing/annual_tariff_activation.py')
        barrier=Barrier(2)
        def worker(_):
            with self.conn.engine.connect() as connection, Session(connection) as session:
                service=types.ModuleType('isolated_concurrent_service')
                service.__dict__.update(db=types.SimpleNamespace(session=session),is_admin=lambda:True,identity=self.review.identity,resolve=self.review.resolve)
                exec(compile(service_tree,'tariff_management.py','exec'),service.__dict__)
                service.text=lambda sql:text(re.sub(r'\bbil_tariff\b',table,re.sub(r'\bbil_tariff_verification\b',audit_table,sql)))
                service.current_user=types.SimpleNamespace(id=7,is_authenticated=True)
                service.today=lambda:date(2026,11,10)
                approval=types.ModuleType('isolated_concurrent_approval')
                approval.__dict__.update({key:getattr(self.review,key) for key in ('identity','resolve','candidates','component','decimal','token')})
                exec(compile(approval_tree,'annual_tariff_activation.py','exec'),approval.__dict__)
                approval.current_user=types.SimpleNamespace(id=7)
                with self.app.test_request_context():
                    prepared=approval.plan(service.records(),service.verification_records(),'2026/27','S01',service.today())
                    signed=approval.confirmation_token(prepared)
                    barrier.wait(timeout=10)
                    try:
                        approval.activate(service,'2026/27','S01',signed,'yes','R/kL')
                        connection.commit();return 'saved'
                    except ValueError:
                        session.rollback();connection.rollback();return 'rejected'
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(worker,range(2)))
        self.assertCountEqual(results,['saved','rejected'])
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM '+table)).scalar(),2)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM '+audit_table)).scalar(),1)

    def test_browser_confirmation_cancel_and_explicit_activation(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlsplit
        from test_pm_admin_landing import ROOT
        self.seed_band('S01')
        self.conn.execute(text('UPDATE bil_tariff SET unit=NULL WHERE id=10'));self.conn.commit()
        before=self.snapshot()
        def route_request(route):
            url=urlsplit(route.request.url)
            # Follow the real Flask redirect within this offline transport;
            # Playwright does not re-route redirected fake-host requests.
            response=self.client.open(url.path+('?' + url.query if url.query else ''),method=route.request.method,
                                      data=route.request.post_data,content_type='application/x-www-form-urlencoded',follow_redirects=True)
            headers={'Content-Type':'text/html; charset=utf-8'}
            if response.location:headers['Location']=response.location
            route.fulfill(status=response.status_code,body=response.data,headers=headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport=dict(width=1440,height=1000))
                page.route('http://tariff-activation.test/**',route_request)
                page.goto('http://tariff-activation.test'+self.url+'?financial_year=2026/27&workspace=register')
                page.locator('[data-component="S01"]').get_by_role('link',name='Approve and Activate').click()
                self.assertIn('R6.5300',page.locator('body').inner_text())
                self.assertIn('Historical unit: not recorded',page.locator('body').inner_text())
                self.assertFalse(page.locator('[name="unit_confirmed"]').is_checked())
                self.assertIn('superseded',page.locator('body').inner_text())
                artifact=ROOT/'output/annual_tariff_register_stage2';artifact.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(artifact/'confirmation-desktop.png'),full_page=True)
                page.get_by_role('link',name='Cancel',exact=True).click()
                self.assertEqual(before,self.snapshot())
                page.locator('[data-component="S01"]').get_by_role('link',name='Approve and Activate').click()
                page.locator('[name="unit_confirmed"]').check()
                page.locator('[name="confirmed"]').check()
                page.get_by_role('button',name='Confirm Approve and Activate').click()
                page.locator('[data-component="S01"]').wait_for()
                self.assertIn('approved and activated',page.locator('body').inner_text())
                self.assertIn('R6.5300',page.locator('[data-component="S01"]').inner_text())
            finally:browser.close()

    def test_blank_historical_units_are_stored_only_on_confirmed_new_versions(self):
        for code,old_unit in [('W01',None),('W02',''),('S01','   '),('S04',None)]:
            with self.subTest(code=code,old_unit=old_unit):
                identifier=10+int(code[-1])+(10 if code.startswith('S') else 0)
                self.seed_band(code,identifier)
                self.conn.execute(text('UPDATE bil_tariff SET unit=:unit WHERE id=:id'),dict(unit=old_unit,id=identifier));self.conn.commit()
                before=self.snapshot()['bil_tariff']
                fields=self.approval(code)
                self.assertEqual(before,self.snapshot()['bil_tariff'])
                response=self.submit(code,dict(fields,unit='kWh'))
                self.assertEqual(response.status_code,302,response.text)
                rows=self.snapshot()['bil_tariff']
                for original in before:self.assertIn(original,rows)
                new=next(row for row in rows if row['code']==code and row['effective_date']=='2026-07-01')
                self.assertEqual(new['unit'],'kL')
                audit=self.conn.execute(text('SELECT source_reference FROM bil_tariff_verification WHERE tariff_id=:id'),dict(id=new['id'])).scalar()
                self.assertIn('confirmed rate unit R/kL; new version unit kL',audit)

    def test_unit_confirmation_is_explicit_and_unsupported_unit_values_are_rejected(self):
        self.seed_band('S01')
        self.conn.execute(text('UPDATE bil_tariff SET unit=NULL WHERE id=10'));self.conn.commit()
        fields=self.approval('S01');before=self.snapshot()
        for value in (None,'','kWh','kL/day','kL'):
            with self.subTest(value=value):
                submitted=dict(fields)
                if value is None:submitted.pop('unit_confirmed')
                else:submitted['unit_confirmed']=value
                response=self.submit('S01',submitted)
                self.assertEqual(response.status_code,409)
                self.assertIn('R/kL rate unit is required',response.text)
                self.assertEqual(before,self.snapshot())

    def test_blank_unit_cannot_bypass_conflicting_mapping_or_band_checks(self):
        self.seed_band('W01')
        self.conn.execute(text("UPDATE bil_tariff SET unit=NULL,utility_type='sewerage' WHERE id=10"));self.conn.commit()
        before=self.snapshot()
        response=self.client.get(self.path('W01'))
        self.assertEqual(response.status_code,400);self.assertIn('component mapping conflicts',response.text)
        self.assertEqual(before,self.snapshot())
        self.conn.execute(text("UPDATE bil_tariff SET utility_type='water',block_start=999 WHERE id=10"));self.conn.commit()
        before=self.snapshot();response=self.client.get(self.path('W01'))
        self.assertEqual(response.status_code,400);self.assertIn('band threshold conflicts',response.text)
        self.assertEqual(before,self.snapshot())

    def test_unit_metadata_change_after_review_requires_a_fresh_confirmation(self):
        self.seed_band('S01')
        self.conn.execute(text('UPDATE bil_tariff SET unit=NULL WHERE id=10'));self.conn.commit()
        fields=self.approval('S01')
        # Simulate a separately committed change while the administrator's form is open.
        self.conn.execute(text("UPDATE bil_tariff SET unit='kL' WHERE id=10"));self.conn.commit()
        before=self.snapshot();response=self.submit('S01',fields)
        self.assertEqual(response.status_code,409);self.assertIn('changed',response.text)
        self.assertEqual(before,self.snapshot())

    def test_backdated_annual_version_does_not_replace_a_later_period(self):
        self.seed_band('S01')
        self.seed_band('S01',11,'8.00','2027-07-01')
        fields=self.approval('S01');before=self.snapshot()['bil_tariff']
        self.assertEqual(self.submit('S01',fields).status_code,302)
        rows=self.snapshot()['bil_tariff']
        for row in before:self.assertIn(row,rows)
        chosen,_=self.review.resolve([row for row in rows if row['code']=='S01'],date(2027,7,1))
        self.assertEqual(chosen[0]['id'],11)

    def test_admin_access_and_unverified_units(self):
        self.seed_band('S01');fields=self.approval('S01');before=self.snapshot()
        self.conn.execute(text("UPDATE bil_tariff SET unit='kWh' WHERE id=10"));self.conn.commit()
        blocked=self.client.get(self.path('S01'));self.assertEqual(blocked.status_code,400)
        self.assertIn('units are unsupported',blocked.text)
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        self.assertEqual(self.client.get(self.path('S01')).status_code,403)
        self.assertEqual(self.submit('S01',fields).status_code,403)
        with self.client.session_transaction() as state:state.clear()
        self.assertEqual(self.client.get(self.path('S01')).status_code,302)

    def test_audit_failure_rolls_back_the_new_tariff(self):
        self.seed_band('S01');fields=self.approval('S01');before=self.snapshot()
        def fail_audit(conn,cursor,sql,parameters,context,many):
            if sql.startswith('INSERT INTO bil_tariff_verification'):raise SQLAlchemyError('Synthetic isolated audit failure')
        event.listen(self.conn,'before_cursor_execute',fail_audit)
        try:response=self.submit('S01',fields)
        finally:event.remove(self.conn,'before_cursor_execute',fail_audit)
        self.assertEqual(response.status_code,503)
        self.assertEqual(before,self.snapshot())

    def test_all_bands_activate_separately_and_use_unchanged_calculators(self):
        for side,rates in [('S',['5.45','9.20','17.54','27.38','34.78']),('W',['39.39','46.70','62.17','95.91','105.39'])]:
            for index,rate in enumerate(rates,1):
                code=side+str(index).zfill(2);self.seed_band(code,10+index+(10 if side=='W' else 0),rate)
                if code!='S05':
                    self.conn.execute(text('UPDATE bil_tariff SET unit=NULL WHERE code=:code'),dict(code=code));self.conn.commit()
                self.assertEqual(self.submit(code,self.approval(code)).status_code,302)
        from pathlib import Path
        from test_pm_admin_landing import ROOT, function
        scope={};exec(compile((ROOT/'app/program_billing/metsoa_sewerage.py').read_text(),'sewerage','exec'),scope)
        rows=self.tariff_service.records();selected,_=self.review.resolve(rows,date(2026,7,1))
        tiers=[dict(code=row['code'],rate=Decimal(str(row['rate'])),label=row['code']) for row in selected if row['utility_type']=='sewerage']
        lines,total=scope['calculate'](175,32,tiers,None,dict(test_mode=True,first_band_exempt=False))
        self.assertEqual([line['cons'] for line in lines],list(map(Decimal,['6.08','15.192','4.008','10.4','76.2'])))
        self.assertEqual([line['rate'] for line in lines],list(map(Decimal,['6.53','11','21','32.8','37.6'])))
        env={};builder=function('app/program_billing/routes.py','build_ws_breakdown',env)
        water=[dict(multiplier=Decimal(str(row['reduction_factor'])),rate=Decimal(str(row['rate'])),label=row['code']) for row in sorted(selected,key=lambda row:row['code']) if row['utility_type']=='water']
        lines,total=builder(Decimal(53),30,'2026-10',1,tiers=water,strict=True)
        self.assertEqual(sum(line['cons'] for line in lines),53)
        self.assertEqual([line['cons'] for line in lines],list(map(Decimal,['6','18.99','5.01','15','8'])))
        old,_=self.review.resolve(rows,date(2026,6,30))
        self.assertEqual(Decimal(str(next(row for row in old if row['code']=='S04')['rate'])),Decimal('27.38'))


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ActivationTests(name) for name in ActivationTests.__dict__ if name.startswith('test_'))
