"""Current relationship/role authority, never inferred from legacy role fields."""
from flask import abort, render_template, redirect, url_for, request
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, OperationalError
from wtforms import IntegerField, SelectField, StringField, BooleanField
from wtforms.validators import InputRequired, NumberRange, Length, Optional
from app.extensions import db
from app.models.retire import (RetirementOrganisation as Org, RetirementMembership as Member,
    RetirementRole as Role, RetirementRelationship as Relationship,
    RetirementStaffRoleAssignment as Assignment, RetirementAuthorityEvent as Event,
    STAFF_ROLE_CODES, utcnow)
from app.models.retire import RetirementWaitingUser
from . import retire_bp

KINDS=('staff','resident','family_representative')


def is_owner(member):
    return bool(member and member.organisation.owner_user_id==member.user_id and
        member.status=='active' and member.approved_role and member.approved_role.code=='organisation_owner')


def staff_eligible(member):
    if not member or member.association_approved_at is None:return False
    return db.session.query(Assignment.id).join(Relationship,Assignment.relationship_id==Relationship.id).join(Role,Assignment.role_id==Role.id).filter(
        Relationship.membership_id==member.id, Relationship.kind=='staff', Relationship.withdrawn_at.is_(None),
        Assignment.withdrawn_at.is_(None), Role.code.in_(STAFF_ROLE_CODES)).first() is not None


def owner_for(home_id, lock=False):
    query=Org.query.filter_by(id=home_id)
    if lock:query=query.with_for_update().populate_existing()
    home=query.first_or_404()
    query=Member.query.filter_by(organisation_id=home_id,user_id=current_user.id)
    if lock:query=query.with_for_update().populate_existing()
    owner=query.first()
    if home.owner_user_id!=current_user.id or not is_owner(owner):abort(403)
    return home


def revision(member_id):
    # Concurrency token only; no authority is reconstructed from events.
    return db.session.query(func.coalesce(func.max(Event.version),0)).filter_by(membership_id=member_id).scalar()


def mutate(home_id, member_id, expected, action, kind=None, target=None, role_id=None, reason='', acknowledge=False):
    """One transaction; identity always comes from the authenticated request."""
    try:
        owner_for(home_id,lock=True)
        member=Member.query.filter_by(id=member_id,organisation_id=home_id).with_for_update().populate_existing().first_or_404()
        version=revision(member.id)
        if expected!=version:abort(409,description='Authority changed. Refresh before submitting again.')
        if not member.association_approved_at:abort(409,description='Approve the home association first.')
        if action.startswith('grant_') and member.status in ('pending','denied','disabled') and not acknowledge:
            abort(409,description='Explicitly acknowledge the legacy decision before granting new authority.')
        now=utcnow()
        def event(rel,assignment,verb,why):
            nonlocal version
            version+=1
            db.session.add(Event(membership_id=member.id,relationship_id=rel.id,
                assignment_id=assignment.id if assignment else None,action=verb,
                actor_user_id=current_user.id,recorded_at=now,version=version,reason=why))
        def close(obj):
            obj.withdrawn_at=now;obj.withdrawn_by_user_id=current_user.id;obj.withdrawal_reason=reason
        if action=='grant_relationship':
            if kind not in KINDS:abort(400)
            existing=Relationship.query.filter_by(membership_id=member.id,kind=kind,withdrawn_at=None).first()
            if existing:return False
            rel=Relationship(membership_id=member.id,kind=kind,granted_at=now,granted_by_user_id=current_user.id,recorded_at=now,origin='owner')
            db.session.add(rel);db.session.flush();event(rel,None,'relationship_granted',reason or 'Explicit owner grant')
        elif action in ('withdraw_relationship','grant_role','withdraw_role'):
            if action=='withdraw_role':
                assignment=Assignment.query.join(Relationship,Assignment.relationship_id==Relationship.id).filter(Assignment.id==target,Relationship.membership_id==member.id).with_for_update(of=Assignment).first_or_404()
                rel=db.session.get(Relationship,assignment.relationship_id)
            else:
                rel=Relationship.query.filter_by(id=target,membership_id=member.id).with_for_update().first_or_404()
            if rel.withdrawn_at is not None:abort(409,description='Relationship has been withdrawn.')
            if action=='grant_role':
                role=db.session.get(Role,role_id)
                if rel.kind!='staff' or not role or role.code not in STAFF_ROLE_CODES:abort(400)
                if Assignment.query.filter_by(relationship_id=rel.id,role_id=role_id,withdrawn_at=None).first():return False
                assignment=Assignment(relationship_id=rel.id,role_id=role_id,granted_at=now,granted_by_user_id=current_user.id,recorded_at=now,origin='owner')
                db.session.add(assignment);db.session.flush();event(rel,assignment,'role_granted',reason or 'Explicit owner role grant')
            else:
                if not reason.strip():abort(400,description='Withdrawal reason is required.')
                if action=='withdraw_role':
                    if assignment.withdrawn_at is not None:abort(409)
                    close(assignment);event(rel,assignment,'role_withdrawn',reason)
                else:
                    for assignment in Assignment.query.filter_by(relationship_id=rel.id,withdrawn_at=None).with_for_update().all():
                        close(assignment);event(rel,assignment,'role_withdrawn',reason)
                    db.session.flush()  # Close child roles before the relationship guard.
                    close(rel);event(rel,None,'relationship_withdrawn',reason)
        else:abort(400)
        db.session.commit()
        return True
    except (IntegrityError,OperationalError) as error:
        db.session.rollback()
        if isinstance(error,IntegrityError) or getattr(error.orig,'pgcode',None) in ('55P03','40P01','40001'):
            abort(409,description='Conflicting authority change. Refresh and try again.')
        raise
    except Exception:
        db.session.rollback()
        raise


class AuthorityForm(FlaskForm):
    version=IntegerField('Revision',validators=[InputRequired(),NumberRange(min=0)])
    action=SelectField('Action',choices=[(s,s.replace('_',' ').title()) for s in ('grant_relationship','withdraw_relationship','grant_role','withdraw_role')])
    kind=SelectField('Relationship',choices=[(s,s.replace('_',' ').title()) for s in KINDS])
    target=IntegerField('Relationship ID (or assignment ID for role withdrawal)',validators=[Optional(),NumberRange(min=1)])
    role_id=SelectField('Staff role',coerce=int,validators=[Optional()])
    reason=StringField('Reason',validators=[Optional(),Length(max=1000)])
    acknowledge=BooleanField('I explicitly authorise this new grant despite the recorded legacy pending/denied/disabled decision')


@retire_bp.route('/organisations/<int:organisation_id>/relationships')
@login_required
def relationships(organisation_id):
    home=owner_for(organisation_id)
    members=Member.query.filter_by(organisation_id=organisation_id).order_by(Member.id).all()
    names=dict(db.session.query(RetirementWaitingUser.user_id,RetirementWaitingUser.preferred_name).filter(RetirementWaitingUser.user_id.in_([m.user_id for m in members])).all())
    return render_template('program_retire/relationships.html',organisation=home,members=members,member=None,names=names)


@retire_bp.route('/organisations/<int:organisation_id>/members/<int:membership_id>/relationships',methods=['GET','POST'])
@login_required
def manage_relationships(organisation_id,membership_id):
    home=owner_for(organisation_id)
    member=Member.query.filter_by(id=membership_id,organisation_id=organisation_id).first_or_404()
    form=AuthorityForm()
    form.role_id.choices=[(0,'Select Staff role')]+[(r.id,r.name) for r in Role.query.filter(Role.code.in_(STAFF_ROLE_CODES)).order_by(Role.id)]
    if request.method=='GET':form.version.data=revision(member.id)
    if request.method=='POST' and any(k in request.form for k in ('actor_user_id','user_id','reviewed_by_user_id','organisation_id','membership_id','granted_by_user_id')):abort(400)
    if form.validate_on_submit():
        mutate(organisation_id,member.id,form.version.data,form.action.data,kind=form.kind.data,target=form.target.data,
               role_id=form.role_id.data,reason=(form.reason.data or '').strip(),acknowledge=form.acknowledge.data)
        return redirect(url_for('retire_bp.manage_relationships',organisation_id=organisation_id,membership_id=member.id))
    rels=Relationship.query.filter_by(membership_id=member.id).order_by(Relationship.id).all()
    assignments=Assignment.query.join(Relationship,Assignment.relationship_id==Relationship.id).filter(Relationship.membership_id==member.id).order_by(Assignment.id).all()
    events=Event.query.filter_by(membership_id=member.id).order_by(Event.version).all()
    person=RetirementWaitingUser.query.filter_by(user_id=member.user_id).first()
    return render_template('program_retire/relationships.html',organisation=home,member=member,form=form,
        relationships=rels,assignments=assignments,events=events,roles={r.id:r.name for r in Role.query.all()},person=person)
