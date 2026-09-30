"""Run unchanged HOME continuation tests with a lifecycle-valid Reading R fixture."""
import importlib.util
from pathlib import Path
import unittest
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('home_continuation_support',ROOT/'tests/support/home_sace_continuation_runner.py')
home=importlib.util.module_from_spec(spec);spec.loader.exec_module(home)

class Isolation(home.Continuation):
    pledge=home.f.h.AccessJourneys.pledge
    def test_cont_stale_litre_controller(self):
        self.user('litre-r@example.test')
        home.f.h.AccessJourneys.provision(self,self.client,'litre-r@example.test',existing=True)
        self.client.get('/logout')
        self.stale(self.client)
        self.assertEqual(self.login(self.client,'litre-r@example.test').location,'/sace/provisioning')
        self.assertEqual(self.client.get('/sace/provisioning').status_code,200)

    def test_litre_authority_code_and_evidence_do_not_grant_home(self):
        self.user('litre@example.test')
        home.f.h.AccessJourneys.provision(self,self.client,'litre@example.test',existing=True)
        code, row_id = home.f.h.AccessJourneys.code(self,self.client)
        with self.app.app_context():
            row=home.f.db.session.get(home.f.h.Interaction,row_id)
            home.f.db.session.add(home.f.h.Interaction(user_id=row.user_id,
                workshop_session_id=home.f.h.endorsement.room(row),activity_slug='map_reviewed',response_data='{}'))
            home.f.db.session.commit()
        self.assertEqual(self.client.get('/sace/home/control').status_code,403)
        self.assertEqual(self.client.post('/sace/home/join',data={'code':code}).status_code,400)
        with self.app.app_context():
            self.assertEqual(home.f.HomeAssignment.query.count(),0)
            self.assertEqual(home.f.HomeEvidence.query.count(),0)
        self.client.get('/logout')
        self.assertEqual(self.login(self.client,'litre@example.test','/sace/reading').location,'/sace/provisioning')

if __name__=='__main__':unittest.main(verbosity=2)
