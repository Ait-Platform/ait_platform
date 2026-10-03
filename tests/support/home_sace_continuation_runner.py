"""Only HOME authentication continuation regressions; temporary local tables only."""
import importlib.util
from pathlib import Path
import unittest
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("home_foundation_support", ROOT / "tests/support/home_sace_postgres_runner.py")
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.program_sace_home import continuation as c
from app.extensions import db


class Continuation(f.HomeFoundation):
    def stale(self, client):
        with client.session_transaction() as state:
            state["sace_home_pending_code"] = "HOME-STALE"
            state["sace_home_provisioning_token"] = "stale-token"
            state.pop(c.KEY, None)

    def active_join(self):
        self.provision_home()
        code = self.code_home()
        self.user("resume-a@example.test")
        client = self.app.test_client()
        client.post("/sace/home/join", data={"code": code})
        client.post("/sace/home/pledge", data={"signature": "HOME A", "accept": "yes"})
        with client.session_transaction() as state:
            self.assertEqual(state[c.KEY]["kind"], "join")
        return client

    def test_cont_explicit_home_next(self):
        self.user("ordinary@example.test")
        result = self.login(self.client, "ordinary@example.test", "/sace/home/")
        self.assertEqual(result.location, "/sace/home/")

    def test_cont_explicit_registration_subject(self):
        self.provision_home()
        with self.app.app_context():
            self.assertEqual(f.HomeController.query.count(), 1)
        with self.client.session_transaction() as state:
            self.assertNotIn(c.KEY, state)

    def test_cont_active_join_without_next(self):
        client = self.active_join()
        result = self.login(client, "resume-a@example.test")
        self.assertEqual(result.location, "/sace/home/claim")
        with client.session_transaction() as state:
            self.assertNotIn(c.KEY, state)
        result = client.get(result.location, follow_redirects=True)
        self.assertEqual(result.status_code, 200)
        self.assertIn(b"HOME Auditor Board", result.data)

    def test_cont_active_provisioning_without_next(self):
        self.user("resume-r@example.test")
        with self.app.app_context():
            token = f.s.issue_provisioning("resume-r@example.test", "test")
            db.session.commit()
        self.client.get("/sace/home/provisioning?token=" + token, follow_redirects=True)
        self.client.post("/sace/home/provisioning", data={"signature": "HOME R", "accept": "yes"})
        result = self.login(self.client, "resume-r@example.test")
        self.assertEqual(result.location, "/sace/home/provisioning")
        self.assertIn(b"HOME Control Centre", self.client.get(result.location, follow_redirects=True).data)
        with self.client.session_transaction() as state:
            self.assertNotIn(c.KEY, state)

    def test_cont_stale_ordinary(self):
        self.user("ordinary@example.test")
        self.stale(self.client)
        self.assertEqual(self.login(self.client, "ordinary@example.test").location, "/dashboard")

    def test_cont_stale_litre_controller(self):
        self.user("litre-r@example.test")
        f.h.AccessJourneys.provision(self, self.client, "litre-r@example.test", existing=True)
        self.client.get("/logout")
        self.stale(self.client)
        self.assertEqual(self.login(self.client, "litre-r@example.test").location, "/sace/provisioning")
        self.assertEqual(self.client.get("/sace/provisioning").status_code, 200)

    def test_cont_coexisting_pending_litre_auditor(self):
        self.user("litre-a@example.test")
        self.stale(self.client)
        with self.client.session_transaction() as state:
            state["pending_sace_code"] = "LITRE-CODE"
            state["sace_evaluator_pledged"] = True
        self.assertEqual(self.login(self.client, "litre-a@example.test").location, "/sace/claim_code")

    def test_cont_litre_entry_abandons_home(self):
        for target in ("/sace/provisioning", "/sace/join"):
            client = self.active_join() if target == "/sace/provisioning" else self.app.test_client()
            if target == "/sace/join":
                with client.session_transaction() as state:
                    state["sace_home_pending_code"] = "HOME-STALE"
                    state[c.KEY] = {"kind": "join", "expires_at": time.time()+900, "pending_hash": f.s.digest("HOME-STALE")}
            response = client.get(target)
            self.assertEqual(response.status_code, 200)
            with client.session_transaction() as state:
                self.assertNotIn(c.KEY, state)

    def test_cont_completed_cannot_recapture_login(self):
        client = self.active_join()
        result = self.login(client, "resume-a@example.test")
        # Do not follow the continuation: it must already be consumed at authentication.
        self.assertEqual(result.location, "/sace/home/claim")
        with client.session_transaction() as state:
            self.assertIn("sace_home_pending_code", state)
            self.assertNotIn(c.KEY, state)
        self.assertEqual(self.login(client, "resume-a@example.test").location, "/dashboard")

    def test_cont_abandoned_expired_and_explicit_nonhome(self):
        client = self.active_join()
        client.get("/")
        with client.session_transaction() as state:
            self.assertNotIn(c.KEY, state)
        self.assertEqual(self.login(client, "resume-a@example.test").location, "/dashboard")
        with client.session_transaction() as state:
            state[c.KEY] = {"kind": "join", "expires_at": time.time()-1, "pending_hash": f.s.digest(state["sace_home_pending_code"])}
        self.assertEqual(self.login(client, "resume-a@example.test").location, "/dashboard")
        with client.session_transaction() as state:
            state[c.KEY] = {"kind": "join", "expires_at": time.time()+900, "pending_hash": f.s.digest(state["sace_home_pending_code"])}
        self.assertEqual(self.login(client, "resume-a@example.test", "/sace/join").location, "/sace/join")
        with client.session_transaction() as state:
            self.assertNotIn(c.KEY, state)


if __name__ == "__main__":
    names = sorted(n for n in dir(Continuation) if n.startswith("test_cont_"))
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Continuation(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
