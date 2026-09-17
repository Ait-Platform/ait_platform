"""Focused Stage 3 tests only. Reuses the local transaction harness, not its suite."""
import unittest
from unittest.mock import patch
import re
import threading
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from flask import Flask
from check_waiting_room_local import (WaitingTests as Harness, app, engine, db, USERS,
    ROOT, login_manager, csrf, retire_bp, datetime, timezone, local)
import retirement_relationship_transition as transition
from app.models.retire import RetirementMembership as Member, RetirementAuthorityEvent as Event
from app.program_retire import authority


class Stage3Tests(unittest.TestCase):
    tearDown=Harness.tearDown
    rows=Harness.rows
    post=Harness.post
    home=Harness.home
    enrol=Harness.enrol
    decide=Harness.decide
    path=Harness.path
    member=Harness.member

    def setUp(self):
        Harness.setUp(self)
        self.enrol();self.h=self.home();self.decide(self.h)
        self.mid=self.member(self.h)['id']
        self.url=f'/retire/organisations/{self.h}/members/{self.mid}/relationships'

    def revision(self):
        return self.conn.execute(sa.text('SELECT coalesce(max(version),0) FROM retirement_authority_event WHERE membership_id=:m'),{'m':self.mid}).scalar_one()

    def change(self,action,**kw):
        data=dict(action=action,kind='staff',version=self.revision(),role_id=0,reason='Focused Stage 3 test')
        data.update(kw)
        return self.post(self.owner,self.url,data)

    def grant(self,kind='staff'):
        self.assertEqual(self.change('grant_relationship',kind=kind).status_code,302)
        return self.rows('retirement_relationship')[-1]['id']

    def role(self,rel,code='care_staff'):
        self.assertEqual(self.change('grant_role',target=rel,role_id=self.roles[code]).status_code,302)
        return self.rows('retirement_staff_role_assignment')[-1]['id']

    def eligible(self):
        with app.app_context():return authority.staff_eligible(db.session.get(Member,self.mid))

    def test_staff_and_family_are_independent(self):
        staff=self.grant();family=self.grant('family_representative');self.role(staff)
        self.assertEqual(self.change('withdraw_relationship',target=family).status_code,302)
        self.assertTrue(self.eligible())
        self.assertIsNone(self.rows('retirement_relationship')[0]['withdrawn_at'])

    def test_staff_withdrawal_closes_roles_not_other_relationships(self):
        staff=self.grant();self.role(staff);self.role(staff,'facility_manager')
        self.grant('resident');self.grant('family_representative')
        self.assertEqual(self.change('withdraw_relationship',target=staff).status_code,302)
        self.assertTrue(all(r['withdrawn_at'] for r in self.rows('retirement_staff_role_assignment')))
        rels=self.rows('retirement_relationship')
        self.assertIsNone(rels[1]['withdrawn_at']);self.assertIsNone(rels[2]['withdrawn_at'])
        self.assertFalse(self.eligible())
        self.assertEqual(len(self.rows('retirement_authority_event')),8)

    def test_multiple_roles_withdraw_one_preserves_other(self):
        staff=self.grant();a=self.role(staff);b=self.role(staff,'facility_manager')
        self.assertEqual(self.change('withdraw_role',target=a).status_code,302)
        roles=self.rows('retirement_staff_role_assignment')
        self.assertIsNotNone(roles[0]['withdrawn_at']);self.assertIsNone(roles[1]['withdrawn_at']);self.assertTrue(self.eligible())

    def test_regrant_never_revives_old_roles(self):
        staff=self.grant();self.role(staff);self.change('withdraw_relationship',target=staff)
        new=self.grant();self.assertNotEqual(staff,new)
        self.assertFalse(self.eligible())
        self.assertEqual(len(self.rows('retirement_staff_role_assignment')),1)
        self.role(new);self.assertTrue(self.eligible())

    def test_staff_without_role_has_no_eligibility(self):
        self.grant();self.assertFalse(self.eligible())
        self.assertEqual(self.mary.get(f'/retire/organisations/{self.h}/dashboard').status_code,302)

    def test_resident_needs_no_staff_role(self):
        self.grant('resident');self.assertFalse(self.rows('retirement_staff_role_assignment'));self.assertFalse(self.eligible())

    def test_family_needs_no_staff_role_or_resident_data(self):
        self.grant('family_representative');self.assertFalse(self.eligible())
        self.assertFalse(self.rows('retirement_staff_role_assignment'))
        self.assertEqual(self.mary.get(f'/retire/organisations/{self.h}/dashboard').status_code,302)
        self.assertEqual(self.mary.get(f'/retire/organisations/{self.h}/residents').status_code,404)

    def test_owner_does_not_imply_staff(self):
        owner=next(m for m in self.rows('retirement_membership') if m['user_id']==USERS[0])
        with app.app_context():
            member=db.session.get(Member,owner['id'])
            self.assertTrue(authority.is_owner(member));self.assertFalse(authority.staff_eligible(member))
        self.assertFalse(self.rows('retirement_relationship'))

    def test_facility_manager_cannot_grant(self):
        staff=self.grant();self.role(staff,'facility_manager')
        self.assertTrue(self.eligible())
        self.assertEqual(self.mary.get(self.url).status_code,403)
        self.assertEqual(self.mary.get(f'/retire/organisations/{self.h}/relationships').status_code,403)

    def test_cross_home_targets_are_rejected(self):
        other_home=self.home(self.other,'Other home')
        self.assertEqual(self.other.get(self.url).status_code,403)
        self.assertEqual(self.owner.get(f'/retire/organisations/{other_home}/members/{self.mid}/relationships').status_code,403)
        rel=self.grant()
        own_home=self.home(name='Second owned home')
        wrong=f'/retire/organisations/{own_home}/members/{self.mid}/relationships'
        self.assertEqual(self.owner.get(wrong).status_code,404)
        self.assertEqual(self.change('withdraw_role',target=999999).status_code,404)

    def test_csrf_and_forged_actor_write_nothing(self):
        self.assertEqual(self.owner.post(self.url,data={'action':'grant_relationship','kind':'staff','version':0}).status_code,400)
        self.assertEqual(self.change('grant_relationship',actor_user_id=USERS[1]).status_code,400)
        self.assertFalse(self.rows('retirement_relationship'));self.assertFalse(self.rows('retirement_authority_event'))

    def test_duplicate_and_stale_changes(self):
        rel=self.grant();self.assertEqual(self.change('grant_relationship').status_code,302)
        self.assertEqual(len(self.rows('retirement_relationship')),1)
        a=self.role(rel);self.assertEqual(self.change('grant_role',target=rel,role_id=self.roles['care_staff']).status_code,302)
        self.assertEqual(len(self.rows('retirement_staff_role_assignment')),1)
        self.assertEqual(self.change('withdraw_role',target=a,version=0).status_code,409)
        self.assertTrue(self.eligible())

    def test_withdrawal_failure_rolls_back_roles_and_history(self):
        rel=self.grant();self.role(rel);self.role(rel,'facility_manager')
        before=self.rows('retirement_authority_event')
        original=db.session.add
        def fail(obj):
            if isinstance(obj,Event) and obj.action=='relationship_withdrawn':raise RuntimeError('Injected audit failure')
            return original(obj)
        with patch.object(db.session,'add',side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,'Injected audit failure'):self.change('withdraw_relationship',target=rel)
        self.assertTrue(self.eligible());self.assertTrue(all(r['withdrawn_at'] is None for r in self.rows('retirement_staff_role_assignment')))
        self.assertEqual(self.rows('retirement_authority_event'),before)

    def test_grant_failure_rolls_back(self):
        original=db.session.add
        def fail(obj):
            if isinstance(obj,Event):raise RuntimeError('Injected audit failure')
            return original(obj)
        with patch.object(db.session,'add',side_effect=fail):
            with self.assertRaises(RuntimeError):self.grant()
        self.assertFalse(self.rows('retirement_relationship'))

    def test_audit_and_grant_history_are_immutable(self):
        self.grant()
        for sql in ("UPDATE retirement_authority_event SET reason='overwrite'",'DELETE FROM retirement_authority_event','TRUNCATE retirement_authority_event',"UPDATE retirement_relationship SET kind='resident'",'DELETE FROM retirement_relationship'):
            save=self.conn.begin_nested()
            with self.assertRaises(DBAPIError):self.conn.execute(sa.text(sql))
            save.rollback()
        self.assertEqual(self.rows('retirement_relationship')[0]['kind'],'staff')

    def legacy(self,state='active',evidence=True):
        self.conn.execute(sa.text("UPDATE retirement_membership SET status=:s,requested_role_id=:r,approved_role_id=:a,requested_at='2025-01-01T00:00:00Z',reviewed_at=:at,reviewed_by_user_id=:by,review_reason='Existing owner approval' WHERE id=:m"),
            {'s':state,'r':self.roles['care_staff'],'a':self.roles['care_staff'] if state in ('active','disabled') else None,
             'at':'2025-01-02T00:00:00Z' if evidence else None,'by':USERS[0] if evidence else None,'m':self.mid})

    def test_evidenced_legacy_import_idempotent_preserves_fields(self):
        self.legacy();before=self.member(self.h)
        self.assertEqual(transition.import_legacy(self.conn),1)
        self.assertEqual(transition.import_legacy(self.conn),0)
        self.assertEqual(self.member(self.h),before);self.assertTrue(self.eligible())
        rel=self.rows('retirement_relationship')[0]
        self.assertEqual(rel['granted_at'],before['reviewed_at']);self.assertEqual(rel['granted_by_user_id'],USERS[0])
        self.assertEqual(rel['origin'],'legacy_import')
        self.assertTrue(all(e['actor_user_id'] is None for e in self.rows('retirement_authority_event')))

    def test_unevidenced_active_blocks_import(self):
        self.legacy(evidence=False)
        with self.assertRaisesRegex(ValueError,'Reconciliation blocker'):transition.import_legacy(self.conn)
        self.assertFalse(self.rows('retirement_relationship'))

    def test_pending_denied_disabled_not_imported(self):
        for state in ('pending','denied','disabled'):
            self.legacy(state);before=self.member(self.h)
            self.assertEqual(transition.import_legacy(self.conn),0)
            self.assertEqual(self.member(self.h),before);self.assertFalse(self.eligible())
        self.assertFalse(self.rows('retirement_relationship'))

    def test_explicit_grant_acknowledges_legacy_restriction(self):
        self.legacy('disabled');self.assertEqual(self.change('grant_relationship').status_code,409)
        self.assertEqual(self.change('grant_relationship',acknowledge='y').status_code,302)
        self.assertEqual(self.member(self.h)['status'],'disabled')

    def test_legacy_role_cannot_restore_withdrawn_authority(self):
        self.legacy();transition.import_legacy(self.conn)
        assignment=self.rows('retirement_staff_role_assignment')[0]
        self.assertEqual(self.change('withdraw_role',target=assignment['id']).status_code,302)
        self.assertEqual(self.member(self.h)['approved_role_id'],self.roles['care_staff'])
        self.assertFalse(self.eligible())
        self.assertEqual(transition.import_legacy(self.conn),0);self.assertFalse(self.eligible())
        self.assertEqual(self.mary.get(f'/retire/organisations/{self.h}/dashboard').status_code,302)

    def test_legacy_writes_closed(self):
        before=self.member(self.h)
        for path in ('/retire/join',f'/retire/organisations/{self.h}/join',f'/retire/organisations/{self.h}/members/{self.mid}/review'):
            page=self.owner.get(self.url).get_data(as_text=True)
            token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',page).group(1)
            self.assertEqual(self.owner.post(path,data={'csrf_token':token,'decision':'approve','role_id':self.roles['care_staff']}).status_code,409)
        self.assertEqual(self.member(self.h),before)

    def test_roles_only_under_current_staff(self):
        resident=self.grant('resident')
        self.assertEqual(self.change('grant_role',target=resident,role_id=self.roles['care_staff']).status_code,400)
        staff=self.grant();self.change('withdraw_relationship',target=staff)
        self.assertEqual(self.change('grant_role',target=staff,role_id=self.roles['care_staff']).status_code,409)
        staff=self.grant()
        # The owner role is not a valid choice and cannot be assigned as Staff.
        self.assertEqual(self.change('grant_role',target=staff,role_id=self.roles['organisation_owner']).status_code,200)
        self.assertFalse(self.rows('retirement_staff_role_assignment'))

    def test_concurrent_authority_change_is_rejected_safely(self):
        with engine.begin() as c:
            local.identity(c)
            oid=c.execute(sa.text("INSERT INTO retirement_organisation(name,owner_user_id) VALUES ('Stage3 concurrency fixture',:u) RETURNING id"),{'u':USERS[0]}).scalar_one()
            c.execute(sa.text("INSERT INTO retirement_membership(organisation_id,user_id,status,approved_role_id,association_approved_at,association_approved_by_user_id) VALUES (:o,:u,'active',:r,now(),:u)"),{'o':oid,'u':USERS[0],'r':self.roles['organisation_owner']})
            mid=c.execute(sa.text("INSERT INTO retirement_membership(organisation_id,user_id,association_approved_at,association_approved_by_user_id) VALUES (:o,:u,now(),:by) RETURNING id"),{'o':oid,'u':USERS[2],'by':USERS[0]}).scalar_one()
        def cleanup():
            with engine.begin() as c:
                local.identity(c)
                self.assertEqual(c.execute(sa.text('SELECT count(*) FROM retirement_relationship WHERE membership_id=:m'),{'m':mid}).scalar_one(),0)
                c.execute(sa.text('DELETE FROM retirement_membership WHERE organisation_id=:o'),{'o':oid})
                c.execute(sa.text('DELETE FROM retirement_organisation WHERE id=:o'),{'o':oid})
        self.addCleanup(cleanup)
        contender=Flask('stage3_contender',template_folder=str(ROOT/'templates'))
        contender.config.update(SECRET_KEY='local-stage3-test',TESTING=True,SQLALCHEMY_DATABASE_URI=engine.url,WTF_CSRF_ENABLED=True,
            SQLALCHEMY_ENGINE_OPTIONS={'connect_args':{'options':'-c search_path=public -c lock_timeout=250 -c statement_timeout=3000'}})
        db.init_app(contender);login_manager.init_app(contender);csrf.init_app(contender)
        for endpoint,path in [('public_bp.welcome','/'),('public_bp.contact','/contact'),('auth_bp.login','/login'),('auth_bp.logout','/logout')]:
            contender.add_url_rule(path,endpoint,lambda:'test shell')
        contender.jinja_env.globals['now']=lambda:datetime.now(timezone.utc)
        contender.register_blueprint(retire_bp)
        client=contender.test_client()
        with client.session_transaction() as s:s['_user_id']=str(USERS[0]);s['_fresh']=True
        url=f'/retire/organisations/{oid}/members/{mid}/relationships'
        token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',client.get(url).get_data(as_text=True)).group(1)
        self.assertEqual(self.post(self.owner,url,{'action':'grant_relationship','kind':'staff','version':0,'role_id':0}).status_code,302)
        result=[]
        def compete():
            try:result.append(client.post(url,data={'action':'grant_relationship','kind':'staff','version':0,'role_id':0,'csrf_token':token}).status_code)
            except Exception as error:result.append(type(error).__name__)
        thread=threading.Thread(target=compete);thread.start();thread.join(5)
        self.assertFalse(thread.is_alive());self.assertEqual(result,[409])
        self.assertEqual(self.conn.execute(sa.text('SELECT count(*) FROM retirement_relationship WHERE membership_id=:m'),{'m':mid}).scalar_one(),1)

    def test_database_rejects_roles_beneath_nonstaff_and_partial_withdrawal(self):
        resident=self.grant('resident');staff=self.grant();self.role(staff)
        for sql,params in [
            ("INSERT INTO retirement_staff_role_assignment(relationship_id,role_id,granted_at,granted_by_user_id,recorded_at,origin) VALUES (:r,:role,now(),:u,now(),'owner')",{'r':resident,'role':self.roles['care_staff'],'u':USERS[0]}),
            ("UPDATE retirement_relationship SET withdrawn_at=now(),withdrawn_by_user_id=:u,withdrawal_reason='Bad direct close' WHERE id=:r",{'u':USERS[0],'r':staff})]:
            save=self.conn.begin_nested()
            with self.assertRaises(DBAPIError):self.conn.execute(sa.text(sql),params)
            save.rollback()
        self.assertTrue(self.eligible())

    def test_disabled_owner_cannot_use_staff_to_recover_admin(self):
        owner=next(m for m in self.rows('retirement_membership') if m['user_id']==USERS[0])
        self.mid=owner['id'];self.url=f'/retire/organisations/{self.h}/members/{self.mid}/relationships'
        staff=self.grant();self.role(staff,'facility_manager')
        self.conn.execute(sa.text("UPDATE retirement_membership SET status='disabled' WHERE id=:m"),{'m':self.mid})
        self.assertTrue(self.eligible())
        self.assertEqual(self.owner.get(self.url).status_code,403)
        self.assertEqual(self.owner.get(f'/retire/organisations/{self.h}/discovery').status_code,403)
        self.assertNotIn(b'Relationship management',self.owner.get(f'/retire/organisations/{self.h}/dashboard').data)


def load_tests(loader, tests, pattern):
    # Importing a TestCase as a fixture must not select its Stage 2 test methods.
    return loader.loadTestsFromTestCase(Stage3Tests)


if __name__=='__main__':
    unittest.main()
