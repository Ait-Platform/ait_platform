"""Quick automatic calendar-period calculation and real save/reopen smoke check."""
import copy,json,unittest
from lxml import html
from sqlalchemy import text
import test_pm_lease_premises_simple as fixture

class AutomaticPeriodSmokeTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.SimplifiedPremisesSmokeTest.setUp
    sign_in=fixture.SimplifiedPremisesSmokeTest.sign_in
    sql=fixture.SimplifiedPremisesSmokeTest.sql
    payload=fixture.SimplifiedPremisesSmokeTest.payload
    assign=fixture.SimplifiedPremisesSmokeTest.assign
    create=fixture.SimplifiedPremisesSmokeTest.create
    route=fixture.SimplifiedPremisesSmokeTest.route
    post=fixture.SimplifiedPremisesSmokeTest.post
    stored=fixture.SimplifiedPremisesSmokeTest.stored

    def test_calendar_calculation_save_reopen_certificate_and_other_values_preserved(self):
        from app.program_billing import lease_workflow as workflow,lease_standard as standard
        for start,months,end,notice in [('2026-03-01','12','2027-02-28','2026-11-30'),('2024-01-01','2','2024-02-29','2023-11-30'),('2024-02-29','12','2025-02-27','2024-11-27'),('2026-01-31','1','2026-02-27','2025-11-27')]:
            calculated,errors=workflow.calculated_period({'s1-17':months,'s1-18':start},{})
            self.assertEqual(errors,[]);self.assertEqual(calculated['expiry_date'],end);self.assertEqual(calculated['renewal_notice_date'],notice)
        original={'_premises':{'property_name':'Almond','leased_area_sqm':'300'},'_parties':{'saved':'unchanged'},'_amounts':{'saved':'unchanged'},'sc-01':{'value':'Saved special condition'},'s1-19':{'value':'2026-03-05'},'_period':{'legacy_metadata':'Keep'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        data={'period_form_version':'2','s1-17':'12','s1-18':'2026-03-01','s1-20':'2099-01-01','renewal_notice_date':'2098-01-01'}
        self.assertEqual(self.post('period',data).status_code,302)
        saved=self.stored();self.assertEqual(saved['s1-17']['value'],'12 months');self.assertEqual(saved['s1-20']['value'],'2027-02-28')
        self.assertEqual(saved['_period']['renewal_notice_date'],'2026-11-30');self.assertEqual(saved['_period']['duration_months'],12)
        for key,value in original.items():
            if key!='_period':self.assertEqual(saved[key],value)
        self.assertEqual(saved['_period']['legacy_metadata'],'Keep')
        tree=html.fromstring(self.client.get(self.route('particulars/period')).data)
        self.assertEqual(tree.xpath('//*[@id="lease-duration-months"]/@value'),['12'])
        for identifier,value in [('lease-expiry-date','2027-02-28'),('lease-renewal-notice-date','2026-11-30')]:
            node=tree.get_element_by_id(identifier);self.assertIn('readonly',node.attrib);self.assertEqual(node.get('value'),value)
        rows={r['key']:r['value'] for r in standard.certificate_rows(saved)}
        self.assertEqual(rows['duration_months'],'12');self.assertEqual(rows['renewal_notice_date'],'2026-11-30')
        self.assertEqual(standard.certificate_values(saved)['[EXPIRY DATE]'],'2027-02-28')
        before=copy.deepcopy(saved)
        self.assertEqual(self.post('period',dict(data,**{'s1-17':'0'})).status_code,400);self.assertEqual(self.stored(),before)
