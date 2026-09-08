"""UIP operational pages. Mutating services are committed exactly once here."""
import csv
import io
from datetime import datetime, timezone
from flask import abort, g, request, render_template, redirect, url_for, flash, send_file, Response
from flask_login import current_user, login_required
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.core import CoreInteraction, CoreTask
from app.models.uip import (UipSlaPolicy, UipMunicipalReferral, UipFollowUp, UipCommunicationLog,
    UipDocument, UipDocumentFolder, UipDocumentVersion, UipCommitteeMeeting, UipMeetingParticipant,
    UipMemberProfile, UipSurvey, UipSurveyResponse, UipResolution, UipDecisionEvent, UipQuorumRule)
from app.uip import uip_bp
from app.uip.services import audit, providers, sla, reception, documents, governance, operations, routing


def field(name, label, options=None, kind="text", required=True, value=""):
    if options is not None:
        options = [(v, str(v).replace("_", " ").title()) for v in sorted(options)] if isinstance(options, set) else options
    return dict(name=name, label=label, options=options, kind=kind, required=required, value=value)


def form(title, operation, fields, **hidden):
    return dict(title=title, operation=operation, fields=fields, hidden=hidden)


def link(label, endpoint, **kwargs):
    return dict(label=label, href=url_for("uip_bp." + endpoint, org_slug=g.organization.slug, **kwargs))


def page(title, columns, rows, forms=(), notes=()):
    from werkzeug.exceptions import Forbidden
    navigation = []
    for label, endpoint, roles in (
        ("Reception", "reception_page", providers.STAFF), ("SLA", "sla_page", providers.STAFF),
        ("Documents", "documents_page", documents.READERS), ("Meetings", "meetings_page", governance.ADMIN),
        ("Surveys", "surveys_page", governance.READ), ("Decisions", "decisions_page", governance.ADMIN),
        ("Export issues", "operations_export", providers.STAFF)):
        try:
            audit.authorize(g.organization.id, current_user.id, roles)
        except Forbidden:
            continue
        navigation.append(link(label, endpoint))
    return render_template("uip/operations/page.html", org=g.organization, title=title,
        columns=columns, rows=rows, forms=forms, notes=notes, navigation=navigation)


def is_admin():
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(g.organization.id, current_user.id, governance.ADMIN)
        return True
    except Forbidden:
        return False


def save():
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        abort(409, description="A conflicting record already exists. Reload before trying again.")
    flash("Changes recorded.", "success")
    return redirect(request.path)


@uip_bp.route("/<org_slug>/operations/sla", methods=["GET", "POST"])
@login_required
def sla_page(org_slug):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, providers.STAFF)
    if request.method == "POST":
        if request.form.get("operation") == "deactivate":
            sla.deactivate(org, actor, reception.identifier(request.form.get("policy_id")))
            return save()
        sla.configure(org, actor, *(request.form.get(k) for k in
            ("category", "priority", "stage", "target_minutes", "warning_minutes")))
        return save()
    forms = []
    from app.uip.completion_routes import navigation_context
    if "manager" in navigation_context()["uip_roles"]:
        forms.append(form("Configure a target", "configure", [field("category", "Issue category", providers.CATEGORIES),
            field("priority", "Priority", {"LOW", "NORMAL", "HIGH", "URGENT"}), field("stage", "Stage", [(s, s.title()) for s in sla.STAGES]),
            field("target_minutes", "Target (elapsed minutes)", kind="number"),
            field("warning_minutes", "Warning lead time (minutes before target)", kind="number")]))
    policies = UipSlaPolicy.query.filter_by(organization_id=org, is_active=True).all()
    if "manager" in navigation_context()["uip_roles"]:
        for policy in policies:
            forms.append(form(f"Deactivate {policy.category} / {policy.priority} / {policy.stage} policy", "deactivate", [], policy_id=policy.id))
    notes = ["Targets use elapsed UTC time. Unconfigured targets and historical records have no inferred SLA. New configuration applies when a stage starts; existing targets retain their policy.",
        "Starts: acknowledgement and dispatch at intake; acceptance at dispatch; commencement at acceptance; completion at commencement; closure at first completion. Rework does not reset closure. No automatic pauses."]
    notes += [f"{p.category} / {p.priority} / {p.stage}: {p.target_minutes} minutes; warning {p.warning_minutes} minutes before due." for p in policies]
    return page("SLA", ["Issue ID", "Work order ID", "Stage", "Started (UTC)", "Target (UTC)", "State"],
        [(c.interaction_id, c.work_order_id or "—", c.stage, c.started_at, c.target_at, state) for c, state in sla.overview(org, actor)], forms, notes)


@uip_bp.route("/<org_slug>/operations/reception", methods=["GET"])
@login_required
def reception_page(org_slug):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, providers.STAFF)
    issues = CoreInteraction.query.filter_by(organization_id=org).order_by(CoreInteraction.id.desc()).all()
    follows = UipFollowUp.query.filter_by(organization_id=org, completed_at=None).all()
    tasks = CoreTask.query.join(CoreInteraction).filter(CoreInteraction.organization_id == org).all()
    return page("Reception actions", ["Issue", "Category", "Priority", "Status", "Outstanding follow-ups", "Open internal tasks"],
        [(link(ix.reference, "reception_issue", issue_id=ix.id), ix.category, ix.priority, ix.status,
          sum(f.interaction_id == ix.id and f.next_action != "NONE" for f in follows),
          sum(t.interaction_id == ix.id and operations.actionable(t) for t in tasks)) for ix in issues],
        notes=["Open an issue to record follow-up, contact, municipal referral, acknowledgement or an internal task."])


@uip_bp.route("/<org_slug>/operations/reception/<int:issue_id>", methods=["GET", "POST"])
@login_required
def reception_issue(org_slug, issue_id):
    from app.models.uip import UipWorkOrder
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    if request.method == "POST":
        action = request.form.get("operation")
        if action == "follow_up":
            reception.follow_up(org, actor, issue_id, request.form)
        elif action == "acknowledge":
            sla.acknowledge(org, actor, issue_id)
        elif action == "communication":
            reception.communication(org, actor, dict(request.form, interaction_id=issue_id))
        elif action == "referral":
            reception.referral(org, actor, issue_id, request.form.get("department"), request.form.get("due"))
        elif action == "task":
            task = operations.add_task(org, actor, issue_id, request.form.get("title"), request.form.get("description"))
            if request.form.get("due"):
                task.due_date = reception.timestamp(request.form["due"], True).replace(tzinfo=None)
        elif action == "finish_follow_up":
            row = UipFollowUp.query.filter_by(organization_id=org, interaction_id=issue_id,
                                             id=reception.identifier(request.form.get("follow_up_id"))).first_or_404()
            reception.finish_follow_up(org, actor, row.id)
        else:
            abort(400)
        return save()
    linked_orders = [(o.id, o.reference) for o in UipWorkOrder.query.filter_by(organization_id=org, interaction_id=issue_id).all()]
    linked_referrals = [(r.id, r.department) for r in UipMunicipalReferral.query.filter_by(organization_id=org, interaction_id=issue_id).all()]
    forms = [form("Acknowledge issue", "acknowledge", []),
        form("Record follow-up / contact attempt", "follow_up", [field("method", "Method", reception.METHODS),
            field("outcome", "Outcome", reception.OUTCOMES), field("next_action", "Next action", reception.NEXT_ACTIONS),
            field("next_action_at", "Next action due (date/time with UTC offset)", required=False),
            field("occurred_at", "Occurred at (optional; defaults to now, UTC offset required)", required=False),
            field("note", "Operational note", kind="textarea", required=False)]),
        form("Create internal task", "task", [field("title", "Title"), field("description", "Description", kind="textarea", required=False),
            field("due", "Due date/time with UTC offset", required=False)]),
        form("Create municipal referral", "referral", [field("department", "Department"), field("due", "Agreed due date/time with UTC offset (optional)", required=False)]),
        form("Log a communication", "communication", [field("channel", "Channel", reception.METHODS),
            field("direction", "Direction", {"INBOUND", "OUTBOUND"}),
            field("party_classification", "Other party", {"MEMBER", "PROVIDER", "MUNICIPALITY", "STAFF", "OTHER"}),
            field("purpose", "Purpose", {"ACKNOWLEDGEMENT", "DISPATCH", "FOLLOW_UP", "INFORMATION", "GOVERNANCE", "OTHER"}),
            field("status", "Log status (does not send a message)", {"RECORDED", "RECEIVED", "FAILED", "DELIVERY_UNAVAILABLE"}),
            field("work_order_id", "Related work order", [("", "None")] + linked_orders, required=False),
            field("referral_id", "Related municipal referral", [("", "None")] + linked_referrals, required=False),
            field("summary", "Summary", kind="textarea", required=False)])]
    rows = []
    for follow in UipFollowUp.query.filter_by(organization_id=org, interaction_id=issue_id).order_by(UipFollowUp.id.desc()).all():
        rows.append(("Follow-up", follow.occurred_at, follow.method, follow.outcome,
                     "Completed" if follow.completed_at else follow.next_action, follow.next_action_at or "—"))
        if follow.next_action != "NONE" and not follow.completed_at:
            forms.append(form(f"Complete follow-up #{follow.id}", "finish_follow_up", [], follow_up_id=follow.id))
    for ref in UipMunicipalReferral.query.filter_by(organization_id=org, interaction_id=issue_id).all():
        rows.append((link("Municipal referral", "referral_page", referral_id=ref.id), ref.created_at, ref.department,
                     ref.status, ref.municipality_reference or "—", ref.sla_expected_date or "—"))
    for comm in UipCommunicationLog.query.filter_by(organization_id=org, interaction_id=issue_id).all():
        rows.append(("Communication", comm.occurred_at, comm.channel, comm.status, comm.purpose, "—"))
    notes = ["Delivery is unavailable. Recording a communication does not send email, SMS or WhatsApp."]
    notes += [f"Communication #{c.id}: {c.summary}" for c in UipCommunicationLog.query.filter_by(organization_id=org, interaction_id=issue_id).all() if c.summary]
    notes += [f"Previous relevant issue: {i.reference} — {i.title} ({i.status})" for i in reception.relevant_issues(org, actor, issue_id)]
    notes += [f"Eligible provider: {r['provider'].name}; active workload {r['active_workload']}. Staff choose the assignment on the issue page."
              for r in routing.recommend(org, actor, issue_id)]
    notes.append(link("Issue and work orders", "view_interaction", reference=issue.reference))
    return page("Reception — " + issue.reference, ["Record", "Recorded time", "Method / department", "Outcome", "Next action / reference", "Due"], rows, forms, notes)


@uip_bp.route("/<org_slug>/operations/municipal/<int:referral_id>", methods=["GET", "POST"])
@login_required
def referral_page(org_slug, referral_id):
    from app.models.uip import UipReferralEvent
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, ("manager", "receptionist", "committee_member"))
    row = UipMunicipalReferral.query.filter_by(organization_id=org, id=referral_id).first_or_404()
    if request.method == "POST":
        reception.transition_referral(org, actor, referral_id, request.form.get("expected_version"),
            request.form.get("status"), request.form.get("reference"), request.form.get("note"), request.form.get("occurred_at"))
        return save()
    choices = reception.REFERRAL_TRANSITIONS.get(row.status, set())
    if row.status != "CLOSED":
        choices = choices | {row.status}
    forms = [form("Record municipal progress", "transition", [field("status", "New state", choices),
        field("reference", "Municipal reference", required=False, value=row.municipality_reference or ""),
        field("occurred_at", "Occurred at (optional, date/time with UTC offset)", required=False),
        field("note", "Response / reason", kind="textarea", required=False)], expected_version=row.version)] if choices else []
    events = UipReferralEvent.query.filter_by(organization_id=org, referral_id=row.id).order_by(UipReferralEvent.resulting_version).all()
    return page("Municipal referral — " + row.department, ["Version", "Date", "Actor", "From", "To", "Reference", "Note"],
        [(e.resulting_version, e.occurred_at, e.actor_user_id, e.previous_state or "—", e.new_state, e.municipality_reference or "—", e.note or "—") for e in events], forms,
        [link("Related issue", "view_interaction", reference=operations.issue(org, row.interaction_id).reference)])


@uip_bp.route("/<org_slug>/operations/documents", methods=["GET", "POST"])
@login_required
def documents_page(org_slug):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, documents.READERS)
    if request.method == "POST":
        if request.form.get("operation") == "folder":
            documents.folder(org, actor, request.form.get("name"))
            return save()
        row, path = documents.upload(org, actor, request.files.get("file"), request.form)
        try:
            return save()
        except Exception:
            db.session.rollback()
            path.unlink(missing_ok=True)
            raise
    forms = []
    if is_admin():
        folders = [(r.id, r.name) for r in UipDocumentFolder.query.filter_by(organization_id=org).all()]
        forms = [form("Create folder", "folder", [field("name", "Folder name")]),
            form("Upload controlled document", "upload", [field("title", "Title"), field("category", "Document category"),
                field("folder_id", "Folder", [("", "No folder")] + folders, required=False),
                field("access_classification", "Visibility", set(documents.VISIBILITY)),
                field("effective_date", "Effective date", kind="date"), field("file", "File (PDF, PNG, JPEG or text)", kind="file")])]
    return page("Controlled documents", ["Title", "Category", "Visibility", "Current version"],
        [(link(r.title or r.filename, "document_page", document_id=r.id), r.category or "—", r.access_classification, r.current_version) for r in documents.listing(org, actor)], forms)


@uip_bp.route("/<org_slug>/operations/documents/<int:document_id>", methods=["GET", "POST"])
@login_required
def document_page(org_slug, document_id):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, documents.READERS)
    row = UipDocument.query.filter_by(organization_id=org, id=document_id).first_or_404()
    if not documents.accessible(org, actor, row):
        abort(404)
    if request.method == "POST":
        if request.form.get("operation") == "metadata":
            documents.metadata(org, actor, document_id, request.form)
            return save()
        row, path = documents.upload(org, actor, request.files.get("file"), request.form, document_id)
        try:
            return save()
        except Exception:
            db.session.rollback()
            path.unlink(missing_ok=True)
            raise
    versions = UipDocumentVersion.query.filter_by(organization_id=org, document_id=row.id).order_by(UipDocumentVersion.version.desc()).all()
    forms = [form("Replace document (preserves earlier versions)", "replace", [field("effective_date", "Effective date", kind="date"),
        field("replacement_reason", "Replacement reason", kind="textarea"), field("file", "File", kind="file")], expected_version=row.current_version)] if is_admin() else []
    if is_admin():
        folders = [(f.id, f.name) for f in UipDocumentFolder.query.filter_by(organization_id=org).all()]
        forms.append(form("Edit current document metadata", "metadata", [field("title", "Title", value=row.title or row.filename),
            field("category", "Category", value=row.category or ""),
            field("folder_id", "Folder", [("", "No folder")] + folders, required=False, value=row.folder_id or ""),
            field("access_classification", "Visibility", set(documents.VISIBILITY), value=row.access_classification)]))
    return page(row.title or row.filename, ["Version", "Status", "Effective date", "Uploader", "File", "Bytes", "Reason"],
        [(v.version, "Active" if v.version == row.current_version else "Superseded", v.effective_date, v.actor_user_id,
          link(v.filename, "document_download", document_id=row.id, version=v.version), v.size_bytes, v.replacement_reason) for v in versions], forms,
        ["Legacy file metadata without a controlled upload has no downloadable content."] if not versions else [])


@uip_bp.route("/<org_slug>/operations/documents/<int:document_id>/versions/<int:version>/download")
@login_required
def document_download(org_slug, document_id, version):
    path, row = documents.download(g.organization.id, current_user.id, document_id, version)
    response = send_file(path, as_attachment=True, download_name=row.filename, mimetype=row.content_type, conditional=False, max_age=0)
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@uip_bp.route("/<org_slug>/operations/meetings", methods=["GET", "POST"])
@login_required
def meetings_page(org_slug):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, governance.ADMIN)
    if request.method == "POST":
        if request.form.get("operation") == "rule":
            governance.quorum_rule(org, actor, request.form.get("percentage"), request.form.get("minimum"), request.form.get("relationship"))
        elif request.form.get("operation") == "meeting":
            governance.meeting(org, actor, request.form)
        else:
            abort(400)
        return save()
    rule = UipQuorumRule.query.filter_by(organization_id=org).first()
    forms = [form("Configure quorum", "rule", [field("percentage", "Required percentage", kind="number"),
        field("minimum", "Minimum participants", kind="number"), field("relationship", "Eligible dated relationship", {"owner", "occupier"})]),
        form("Schedule meeting", "meeting", [field("title", "Title"), field("meeting_type", "Meeting type"),
            field("scheduled_at", "Date/time with UTC offset"), field("location", "Venue or online details"), field("agenda", "Agenda", kind="textarea")])]
    return page("Meetings and quorum", ["Meeting", "Scheduled (UTC)", "Status", "Eligible", "Present", "Required", "Quorum"],
        [(link(m.title, "meeting_page", meeting_id=m.id), m.scheduled_at, m.status, m.eligible_count, m.attendance_count, m.required_quorum,
          "Not recorded" if m.quorum_achieved is None else "Achieved" if m.quorum_achieved else "Not achieved") for m in UipCommitteeMeeting.query.filter_by(organization_id=org).all()], forms,
        [f"Configured quorum: {rule.percentage}%, minimum {rule.minimum}, relationship {rule.relationship}." if rule else "Quorum configuration is required."])


@uip_bp.route("/<org_slug>/operations/meetings/<int:meeting_id>", methods=["GET", "POST"])
@login_required
def meeting_page(org_slug, meeting_id):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, governance.ADMIN)
    row = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).first_or_404()
    if request.method == "POST":
        action = request.form.get("operation")
        if action == "start":
            governance.start_meeting(org, actor, meeting_id)
        elif action in {"edit", "cancel"}:
            governance.update_meeting(org, actor, meeting_id, request.form, cancel=action == "cancel")
        elif action == "attendance":
            governance.attendance(org, actor, meeting_id, request.form.get("member_id"), request.form.get("status"), request.form.get("proxy_id"))
        elif action == "conclude":
            governance.conclude(org, actor, meeting_id, request.form.get("minutes"))
        else:
            abort(400)
        return save()
    forms = []
    if row.status == "SCHEDULED":
        forms.append(form("Start meeting and capture eligibility", "start", []))
        forms.append(form("Edit scheduled meeting", "edit", [field("title", "Title", value=row.title),
            field("meeting_type", "Meeting type", value=row.meeting_type),
            field("scheduled_at", "Date/time with UTC offset", value=sla.utc(row.scheduled_at).isoformat()),
            field("location", "Venue", value=row.location), field("agenda", "Agenda", kind="textarea", value=row.agenda)]))
        forms.append(form("Cancel scheduled meeting", "cancel", [field("reason", "Cancellation reason", kind="textarea")]))
    if row.status in {"SCHEDULED", "IN_PROGRESS"}:
        rule = UipQuorumRule.query.filter_by(organization_id=org).first()
        basis = row.eligibility_basis if row.eligibility_basis is not None else (
            governance.eligibility(org, row.scheduled_at.date(), rule.relationship) if rule else {})
        all_members = UipMemberProfile.query.filter_by(organization_id=org, is_active=True).all()
        members = [(m.id, m.name) for m in all_members if str(m.id) in basis]
        representatives = [(m.id, m.name) for m in all_members if any(
            m.id == r["member_id"] for b in basis.values() for r in b["representatives"])]
        forms.append(form("Record invitation / attendance", "attendance", [field("member_id", "Member", members),
            field("status", "Attendance", {"INVITED", "PRESENT", "APOLOGY", "ABSENT"} if row.status == "IN_PROGRESS" else {"INVITED", "APOLOGY", "ABSENT"}),
            field("proxy_id", "Attending representative (optional; verified against selected member)", [("", "Member attends directly")] + (representatives if row.status == "IN_PROGRESS" else []), required=False)]))
    if row.status == "IN_PROGRESS":
        forms.append(form("Conclude meeting and freeze quorum", "conclude", [field("minutes", "Minutes and decisions", kind="textarea")]))
    participants = UipMeetingParticipant.query.filter_by(organization_id=org, meeting_id=row.id).all()
    names = {m.id: m.name for m in UipMemberProfile.query.filter_by(organization_id=org).all()}
    return page(row.title, ["Member", "Attendance", "Attending member", "Eligible in snapshot"],
        [(names.get(p.member_id, p.member_id), p.status, names.get(p.attended_by_member_id, "—"), str(p.member_id) in (row.eligibility_basis or {})) for p in participants], forms,
        [row.agenda or "No agenda recorded.", row.minutes_text or "Minutes not yet recorded.",
         "Invitations are recorded here; this does not send an invitation. Only eligible dated register members are offered. Configure quorum and verify member/property relationships if the list is empty.",
         f"Status: {row.status}. Frozen eligible count: {row.eligible_count}; present: {row.attendance_count}; required: {row.required_quorum}; quorum achieved: {row.quorum_achieved}.",
         link("Meetings / quorum configuration", "meetings_page"),
         link("Record formal decision from concluded meeting", "decisions_page", meeting_id=row.id)])


@uip_bp.route("/<org_slug>/operations/surveys", methods=["GET", "POST"])
@login_required
def surveys_page(org_slug):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, governance.READ)
    if request.method == "POST":
        questions = []
        for i in range(1, 4):
            if request.form.get(f"question_{i}"):
                questions.append(dict(title=request.form[f"question_{i}"], type=request.form.get(f"type_{i}"),
                    options=[s.strip() for s in request.form.get(f"options_{i}", "").splitlines() if s.strip()]))
        governance.survey(org, actor, request.form, questions)
        return save()
    fields = [field("title", "Title"), field("purpose", "Purpose", kind="textarea"),
        field("opens_at", "Opening date/time with UTC offset"), field("closes_at", "Closing date/time with UTC offset"),
        field("relationship", "Eligible dated relationship", {"owner", "occupier"}),
        field("identifiable", "May administrators view identifiable responses?", [("no", "No — confidential responses"), ("yes", "Yes — identifiable responses")])]
    for i in range(1, 4):
        fields.extend([field(f"question_{i}", f"Question {i}", required=i == 1),
            field(f"type_{i}", "Answer type", {"SINGLE_CHOICE", "YES_NO", "RATING"}),
            field(f"options_{i}", "Single-choice options (one per line)", kind="textarea", required=False)])
    return page("Surveys", ["Survey", "Opens (UTC)", "Closes (UTC)", "State", "Privacy"],
        [(link(s.title, "survey_page", survey_id=s.id), s.opens_at, s.closes_at, s.status, "Identifiable" if s.identifiable else "Confidential")
         for s in UipSurvey.query.filter_by(organization_id=org).all()], [form("Create survey", "survey", fields)] if is_admin() else [])


@uip_bp.route("/<org_slug>/operations/surveys/<int:survey_id>", methods=["GET", "POST"])
@login_required
def survey_page(org_slug, survey_id):
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, governance.READ)
    row = UipSurvey.query.filter_by(organization_id=org, id=survey_id).first_or_404()
    if request.method == "POST":
        if request.form.get("operation") == "finalize":
            governance.finalize(org, actor, survey_id)
        else:
            governance.respond(org, actor, survey_id, request.form.get("member_id"),
                {q["id"]: request.form.get("answer_" + q["id"]) for q in row.questions})
        return save()
    now = datetime.now(timezone.utc)
    forms, rows = [], []
    response_notes = []
    if row.status == "OPEN" and sla.utc(row.opens_at) <= now < sla.utc(row.closes_at):
        basis = governance.eligibility(org, now.date(), row.relationship)
        active_members = UipMemberProfile.query.filter_by(organization_id=org, is_active=True).all()
        member_names = {m.id: m.name for m in active_members}
        my_members = {m.id for m in active_members if m.membership.user_id == actor}
        eligible = [(b["member_id"], member_names[b["member_id"]]) for b in basis.values()
                    if my_members.intersection({b["member_id"]} | {r["member_id"] for r in b["representatives"]})]
        submitted = {r.member_id for r in UipSurveyResponse.query.filter(UipSurveyResponse.organization_id == org,
            UipSurveyResponse.survey_id == row.id, UipSurveyResponse.member_id.in_([m for m, label in eligible])).all()}
        response_notes = [label + ": response recorded." for m, label in eligible if m in submitted]
        eligible = [(m, label) for m, label in eligible if m not in submitted]
        if eligible:
            forms.append(form("Submit one response per eligible member", "respond", [field("member_id", "Responding for", eligible)] +
                [field("answer_" + q["id"], q["title"], [(o, o) for o in q["options"]]) for q in row.questions]))
    if row.status == "OPEN" and now >= sla.utc(row.closes_at) and is_admin():
        forms.append(form("Finalize results", "finalize", []))
    if row.results:
        rows = [(q["title"], answer, count) for q in row.questions for answer, count in row.results["questions"][q["id"]].items()]
    notes = [row.purpose, "Identifiable responses: administrators can view individual answers." if row.identifiable else "Confidential: only final aggregate results are shown."]
    notes.extend(response_notes)
    if row.identifiable and is_admin() and row.status == "FINALIZED":
        for response in UipSurveyResponse.query.filter_by(organization_id=org, survey_id=row.id).all():
            notes.append(f"Member #{response.member_id}: " + "; ".join(q["title"] + ": " + response.answers[q["id"]] for q in row.questions))
    if row.status == "OPEN" and not forms and not response_notes:
        notes.append("Responses require an open survey and a linked account belonging to an eligible member or their verified dated representative. Administrator roles alone do not qualify.")
    notes.append(link("Survey register", "surveys_page"))
    if is_admin() and row.status == "FINALIZED":
        notes.append(link("Record decision from these results", "decisions_page", survey_id=row.id))
    return page(row.title, ["Question", "Answer", "Count"], rows, forms, notes)


@uip_bp.route("/<org_slug>/operations/decisions", methods=["GET", "POST"])
@login_required
def decisions_page(org_slug):
    from app.uip.routes import _members_with_roles
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, governance.ADMIN)
    if request.method == "POST":
        if request.form.get("operation") == "status":
            governance.decision_status(org, actor, request.form.get("decision_id"), request.form.get("status"), request.form.get("note"))
        elif request.form.get("operation") == "task":
            operations.add_task(org, actor, reception.identifier(request.form.get("interaction_id")),
                                request.form.get("title"), request.form.get("description"))
        else:
            governance.decision(org, actor, request.form)
        return save()
    meetings = [(m.id, m.title) for m in UipCommitteeMeeting.query.filter_by(organization_id=org, status="CONCLUDED", quorum_achieved=True).all()]
    surveys = [(s.id, s.title) for s in UipSurvey.query.filter_by(organization_id=org, status="FINALIZED").all()]
    tasks = [(t.id, t.title) for t in CoreTask.query.join(CoreInteraction).filter(CoreInteraction.organization_id == org).all()]
    responsible = [(u.id, u.name or str(u.id)) for u in _members_with_roles("manager", "committee_member", "receptionist").all()]
    decisions = UipResolution.query.filter_by(organization_id=org).order_by(UipResolution.id).all()
    forms = [form("Record decision or correction", "decision", [field("title", "Title"), field("description", "Decision and voting basis", kind="textarea"),
        field("meeting_id", "Source meeting (choose meeting OR survey)", [("", "None")] + meetings, required=False, value=request.args.get("meeting_id", "")),
        field("survey_id", "Source survey", [("", "None")] + surveys, required=False, value=request.args.get("survey_id", "")),
        field("votes_for", "Meeting votes for (required for a meeting decision)", kind="number", required=False),
        field("votes_against", "Meeting votes against", kind="number", required=False),
        field("abstentions", "Meeting abstentions", kind="number", required=False),
        field("linked_task_id", "Existing internal follow-up task", [("", "None")] + tasks, required=False),
        field("responsible_user_id", "Responsible person", [("", "Recorder is responsible")] + responsible, required=False),
        field("supersedes_id", "Corrects / supersedes decision", [("", "None")] + [(d.id, d.title) for d in decisions], required=False)])]
    rows = []
    for d in decisions:
        events = UipDecisionEvent.query.filter_by(organization_id=org, decision_id=d.id).order_by(UipDecisionEvent.id).all()
        status = events[-1].status if events else d.status
        rows.append((d.id, d.title, d.description, d.decision_date or d.created_at, status, d.responsible_user_id or "Not recorded", d.linked_task_id or "—", d.supersedes_id or "—"))
        if status not in {"COMPLETED", "SUPERSEDED"}:
            forms.append(form(f"Update action status for decision #{d.id}", "status", [field("status", "Status", {"IN_PROGRESS", "COMPLETED"}),
                field("note", "Reason / progress", kind="textarea")], decision_id=d.id))
    from app.uip.completion_routes import navigation_context
    if "manager" in navigation_context()["uip_roles"]:
        issues = [(i.id, i.reference + " — " + i.title) for i in CoreInteraction.query.filter(
            CoreInteraction.organization_id == org, CoreInteraction.status != "RESOLVED").all()]
        forms.append(form("Create internal decision follow-up task", "task", [field("interaction_id", "Related open issue", issues),
            field("title", "Task title"), field("description", "Task details", kind="textarea", required=False)]))
    return page("Governance decisions", ["ID", "Title", "Decision", "Date", "Action status", "Responsible user", "Internal task", "Supersedes"], rows, forms,
        ["Create any required internal task first, then select it while recording the decision. Formal decision content is retained; correct it through supersession."])


@uip_bp.route("/<org_slug>/operations/report.csv")
@login_required
def operations_export(org_slug):
    audit.authorize(g.organization.id, current_user.id, providers.STAFF)
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(["Reference", "Title", "Category", "Priority", "Status", "Created UTC", "Closed UTC"])
    for row in CoreInteraction.query.filter_by(organization_id=g.organization.id).order_by(CoreInteraction.id).all():
        values = [row.reference, row.title, row.category, row.priority, row.status, row.created_at, row.closed_at]
        safe = []
        for value in values:
            value = str(value) if value is not None else ""
            safe.append("'" + value if value.lstrip().startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n")) else value)
        writer.writerow(safe)
    return Response(stream.getvalue(), mimetype="text/csv", headers={"Content-Disposition": 'attachment; filename="uip-issues.csv"', "Cache-Control": "private, no-store"})


@uip_bp.route("/<org_slug>/operations/providers")
@login_required
def provider_performance(org_slug):
    from app.models.uip import UipProvider, UipWorkOrder
    org, actor = g.organization.id, current_user.id
    audit.authorize(org, actor, providers.STAFF)
    orders = UipWorkOrder.query.filter_by(organization_id=org).all()
    clocks = sla.overview(org, actor)
    rows = []
    for provider in UipProvider.query.filter_by(organization_id=org).order_by(UipProvider.name).all():
        assigned = [o for o in orders if o.provider_id == provider.id]
        ids = {o.id for o in assigned}
        measured = [(c, state) for c, state in clocks if c.work_order_id in ids and c.finished_at is not None]
        rows.append((provider.name, "Active" if provider.is_active else "Inactive", provider.availability,
            sum(o.status not in providers.TERMINAL for o in assigned), sum(o.status == "CLOSED" for o in assigned),
            sum(state == "met" for c, state in measured), len(measured)))
    return page("Provider workload and history", ["Provider", "Register status", "Availability", "Active workload", "Closed orders", "Recorded SLA stages met", "Recorded SLA sample size"], rows,
        notes=["Counts reflect recorded work orders and configured SLA stages. No performance ranking or inferred statistics are shown where samples are absent."])
