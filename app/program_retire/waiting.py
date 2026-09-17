"""Limited discovery and home permission; never grants operational authority."""
from flask import abort, g, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from sqlalchemy import select, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError
from wtforms import BooleanField, IntegerField, SelectField, StringField, SubmitField
from wtforms.validators import InputRequired, Length, NumberRange, Optional, DataRequired

from app.extensions import db
from app.models.retire import (RetirementWaitingUser as Waiting, RetirementMembership as Member,
                              RetirementAssociationReview as Review, utcnow)
from . import retire_bp
from .routes import active_membership_required, _require_owner


class WaitingForm(FlaskForm):
    preferred_name = StringField('Your recognisable name', filters=[lambda s: s.strip() if s else s], validators=[DataRequired(), Length(max=200)])
    home_name_clue = StringField('Retirement-home name, if known', filters=[lambda s: s.strip() if s else None], validators=[Optional(), Length(max=200)])
    discovery_consent = BooleanField('I consent to my name and optional home-name clue being discoverable by authorised RCM home administrators')
    submit = SubmitField('Save discovery preferences')


class DecisionForm(FlaskForm):
    version = IntegerField('Decision version', validators=[InputRequired(), NumberRange(min=0)])
    decision = SelectField('Decision', choices=[('approve', 'Approve association'), ('not_ours', 'Not ours')], validators=[DataRequired()])
    recognise = BooleanField('I recognise this person and confirm their association with this home')
    reconsider = BooleanField('Explicitly reconsider our previous Not ours decision')
    submit = SubmitField('Save decision')


def reject_identity_fields():
    if any(k in request.form for k in ('user_id', 'reviewer_id', 'reviewed_by_user_id', 'organisation_id', 'home_id', 'approved_role_id', 'status')):
        abort(400)


@retire_bp.route('/waiting-room', methods=['GET', 'POST'])
@login_required
def waiting_room():
    person = Waiting.query.filter_by(user_id=current_user.id).first()
    form = WaitingForm(obj=person)
    if request.method == 'POST':
        reject_identity_fields()
    if form.validate_on_submit():
        if person is None and not form.discovery_consent.data:
            form.discovery_consent.errors.append('Consent is required to enter discovery. You may withdraw it later.')
        else:
            try:
                now = utcnow()
                db.session.execute(insert(Waiting).values(user_id=current_user.id,
                    preferred_name=form.preferred_name.data, home_name_clue=form.home_name_clue.data,
                    discovery_consent=form.discovery_consent.data, consent_updated_at=now,
                    created_at=now, updated_at=now).on_conflict_do_nothing(index_elements=['user_id']))
                person = Waiting.query.filter_by(user_id=current_user.id).with_for_update().populate_existing().one()
                if person.discovery_consent != form.discovery_consent.data:
                    person.consent_updated_at = now
                person.preferred_name = form.preferred_name.data
                person.home_name_clue = form.home_name_clue.data or None
                person.discovery_consent = form.discovery_consent.data
                person.updated_at = now
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise
            return redirect(url_for('retire_bp.waiting_room'))
    associations = Member.query.filter_by(user_id=current_user.id).filter(Member.association_approved_at.isnot(None)).all()
    return render_template('program_retire/waiting_room.html', form=form, person=person, associations=associations)


@retire_bp.route('/organisations/<int:organisation_id>/discovery')
@active_membership_required
def discovery(organisation_id):
    _require_owner()
    phrase = request.args.get('q', '').strip()[:200]
    page = max(1, min(request.args.get('page', 1, type=int) or 1, 10000))
    dismissed = request.args.get('dismissed') == '1'
    latest = (select(Review.decision).where(Review.organisation_id == organisation_id,
                Review.waiting_user_id == Waiting.id).order_by(Review.version.desc()).limit(1).correlate(Waiting).scalar_subquery())
    approved = select(Member.id).where(Member.organisation_id == organisation_id,
                Member.user_id == Waiting.user_id, Member.association_approved_at.isnot(None)).exists()
    query = db.session.query(Waiting.id, Waiting.preferred_name, Waiting.home_name_clue).filter(
        Waiting.discovery_consent.is_(True), ~approved, Waiting.user_id != current_user.id)
    query = query.filter(latest == 'not_ours') if dismissed else query.filter(latest.is_(None))
    if phrase:
        escaped = phrase.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        query = query.filter(or_(Waiting.preferred_name.ilike('%' + escaped + '%', escape='\\'),
                                Waiting.home_name_clue.ilike('%' + escaped + '%', escape='\\')))
    people = query.order_by(Waiting.preferred_name, Waiting.id).offset((page - 1) * 25).limit(26).all()
    return render_template('program_retire/discovery.html', organisation=g.retirement_organisation,
                           people=people[:25], more=len(people)>25, page=page, phrase=phrase, dismissed=dismissed)


@retire_bp.route('/organisations/<int:organisation_id>/discovery/<int:waiting_id>', methods=['GET', 'POST'])
@active_membership_required
def association_review(organisation_id, waiting_id):
    _require_owner()
    form = DecisionForm()
    try:
        if request.method == 'POST':
            reject_identity_fields()
            # Lock/recheck the reviewer's authority as well as the candidate.
            owner = Member.query.filter_by(organisation_id=organisation_id, user_id=current_user.id).with_for_update().populate_existing().one()
            if owner.status != 'active' or owner.approved_role_id is None:
                abort(403)
        candidate = Waiting.query.filter_by(id=waiting_id)
        if request.method == 'POST':
            candidate = candidate.with_for_update().populate_existing()
        person = candidate.first_or_404()
        if not person.discovery_consent or person.user_id == current_user.id:
            abort(404)
        history = Review.query.filter_by(organisation_id=organisation_id, waiting_user_id=waiting_id).order_by(Review.version.desc()).all()
        latest = history[0] if history else None
        version = latest.version if latest else 0
        if request.method == 'GET':
            form.version.data = version
        if form.validate_on_submit():
            if form.version.data != version:
                if latest and form.version.data == version - 1 and latest.decision == form.decision.data:
                    return redirect(url_for('retire_bp.discovery', organisation_id=organisation_id))
                abort(409, description='Decision changed. Refresh before reviewing again.')
            if latest:
                if latest.decision == 'approve':
                    abort(409, description='Association already approved. Discovery cannot revoke it.')
                if not form.reconsider.data:
                    abort(409, description='Explicit reconsideration is required.')
            member = Member.query.filter_by(organisation_id=organisation_id, user_id=person.user_id).with_for_update().first()
            if member and member.association_approved_at is not None:
                abort(409, description='Association already approved.')
            if form.decision.data == 'approve' and not form.recognise.data:
                form.recognise.errors.append('Confirm that you recognise this person. Names alone are not proof.')
            else:
                now = utcnow()
                if form.decision.data == 'approve':
                    db.session.execute(insert(Member).values(organisation_id=organisation_id, user_id=person.user_id,
                        status=None, association_approved_at=now, association_approved_by_user_id=current_user.id,
                        requested_at=now).on_conflict_do_nothing(index_elements=['organisation_id', 'user_id']))
                    member = Member.query.filter_by(organisation_id=organisation_id, user_id=person.user_id).with_for_update().populate_existing().one()
                    if member.association_approved_at is None:
                        member.association_approved_at = now
                        member.association_approved_by_user_id = current_user.id
                db.session.add(Review(organisation_id=organisation_id, waiting_user_id=waiting_id,
                    decision=form.decision.data, reviewed_by_user_id=current_user.id, reviewed_at=now, version=version+1))
                db.session.commit()
                return redirect(url_for('retire_bp.discovery', organisation_id=organisation_id))
        return render_template('program_retire/association_review.html', form=form,
            organisation=g.retirement_organisation, person=person, history=history)
    except OperationalError as error:
        db.session.rollback()
        if getattr(error.orig, 'pgcode', None) in ('55P03', '40P01', '40001'):
            abort(409, description='Another review is in progress. Refresh and try again.')
        raise
    except Exception:
        db.session.rollback()
        raise
