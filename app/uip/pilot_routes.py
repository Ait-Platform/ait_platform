"""UIP pilot completion: explicit mail delivery, scoped Help and advisory AI."""
import uuid
import os
from collections import Counter
from flask import abort,g,request,render_template,url_for,redirect,current_app,flash
from flask_login import current_user,login_required
from itsdangerous import URLSafeTimedSerializer,BadSignature
from app.extensions import db
from app.models.core import CoreOrganization,CoreOrganizationWallet,CoreOrganizationLedger,CoreAiRequest
from app.uip import uip_bp
from app.uip.services import audit,governance,invitations,ai
from app.services import ait_ai_gateway as gateway


def serializer():
    return URLSafeTimedSerializer(current_app.secret_key,salt="uip-voting-preview-v1")


@uip_bp.route("/<org_slug>/operations/surveys/<int:survey_id>/invitations",methods=["GET","POST"])
@login_required
def voting_invitations(org_slug,survey_id):
    org,actor=g.organization,current_user.id
    row,rows=invitations.roster(org.id,actor,survey_id)
    member_id=request.args.get("member",type=int)
    selected=invitations.plan(rows,member_id)
    payload=dict(org=org.id,survey=survey_id,actor=actor,selected=selected)
    if request.method=="POST":
        try: approved=serializer().loads(request.form.get("confirmation",""),max_age=900)
        except BadSignature: abort(400,description="Preview expired. Review the invitations again.")
        if approved!=payload: abort(409,description="Recipients changed. Review a fresh preview before sending.")
        if not selected: abort(409,description="No eligible recipients to send.")
        if not current_app.config.get("UIP_PUBLIC_BASE_URL"):
            current_app.config["UIP_PUBLIC_BASE_URL"]=os.environ.get("UIP_PUBLIC_BASE_URL","")
        invitations.send(org,actor,survey_id,selected)
        flash("Invitation attempts recorded. Check the status below; accepted does not mean delivered or read.","info")
        return redirect(url_for("uip_bp.voting_invitations",org_slug=org.slug,survey_id=survey_id))
    counts=dict(eligible=len(rows),with_email=sum(bool(r["email"]) for r in rows),without_email=sum(not r["email"] for r in rows),
        voted=sum(r["voted"] for r in rows),not_voted=sum(not r["voted"] for r in rows),
        not_sent=sum(not r["invitation"] for r in rows),sent=sum(bool(r["invitation"] and r["invitation"].status=="ACCEPTED") for r in rows),
        failed=sum(bool(r["invitation"] and r["invitation"].status=="FAILED") for r in rows))
    return render_template("uip/invitations.html",org=org,survey=row,rows=rows,counts=counts,selected=selected,confirmation=serializer().dumps(payload))


@uip_bp.route("/vote/<public_org_slug>/<int:survey_id>",methods=["GET","POST"])
def public_vote(public_org_slug,survey_id):
    # Deliberately no org_slug context or general UIP authentication/navigation.
    org=CoreOrganization.query.filter_by(slug=public_org_slug).first_or_404()
    row=None; token=""; done=False
    if request.method=="POST":
        token=request.form.get("token","")
        row,invite,basis=invitations.entitlement(org.id,survey_id,token)
        if request.form.get("operation")=="vote":
            invitations.vote(org.id,survey_id,token,{q["id"]:request.form.get("answer_"+q["id"]) for q in row.questions})
            db.session.commit(); done=True; token=""
    response=current_app.make_response(render_template("uip/public_vote.html",org=org,survey=row,token=token,done=done))
    response.headers["Cache-Control"]="no-store"
    response.headers["Referrer-Policy"]="no-referrer"
    response.headers["X-Robots-Tag"]="noindex, nofollow"
    return response


@uip_bp.route("/<org_slug>/help")
@login_required
def help_page(org_slug):
    return render_template("uip/help.html",org=g.organization)


@uip_bp.route("/<org_slug>/administration/ai-wallet",methods=["GET","POST"])
@login_required
def ai_wallet(org_slug):
    org,actor=g.organization.id,current_user.id
    audit.authorize(org,actor,("manager",))
    if request.method=="POST":
        row=gateway.allocate(org,actor,request.form.get("amount"),request.form.get("reason"),request.form.get("request_key"))
        if not getattr(row,"_uip_audited",False):
            # Existing ledger references are checked rather than duplicated on retry.
            from app.models.uip import UipAuditEvent
            if not UipAuditEvent.query.filter_by(organization_id=org,action="wallet.allocated",entity_type=type(row).__name__,entity_id=row.id).first():
                ai.event(org,actor,"wallet.allocated",row)
        db.session.commit()
        return redirect(url_for("uip_bp.ai_wallet",org_slug=org_slug))
    wallet=CoreOrganizationWallet.query.filter_by(organization_id=org).first()
    usage=CoreAiRequest.query.filter_by(organization_id=org,product="uip").order_by(CoreAiRequest.id.desc()).limit(50).all()
    ledger=CoreOrganizationLedger.query.filter_by(wallet_id=wallet.id,product="uip").order_by(CoreOrganizationLedger.id.desc()).limit(50).all() if wallet else []
    feature_usage=db.session.query(CoreAiRequest.feature,db.func.count(),db.func.sum(CoreAiRequest.credits)).filter_by(organization_id=org,product="uip",status="completed").group_by(CoreAiRequest.feature).all()
    return render_template("uip/ai_wallet.html",org=g.organization,wallet=wallet,usage=usage,ledger=ledger,feature_usage=feature_usage,config=gateway.configuration(),can_allocate=gateway.platform_admin(actor),request_key=str(uuid.uuid4()))


@uip_bp.route("/<org_slug>/assistant",methods=["GET","POST"])
@login_required
def ai_assistant(org_slug):
    audit.authorize(g.organization.id,current_user.id,ai.ROLES)
    result=None
    if request.method=="POST":
        if request.form.get("approved")!="yes": abort(400,description="Confirm the source text is appropriate to share with the AI provider.")
        result=ai.run(g.organization.id,current_user.id,request.form.get("feature"),request.form.get("context"),request.form.get("request_key"),request.form.get("issue_id",type=int))
    return render_template("uip/assistant.html",org=g.organization,tasks=ai.TASKS,result=result,request_key=str(uuid.uuid4()),config=gateway.configuration(),selected_feature=request.values.get("feature"),selected_issue=request.values.get("issue_id", ""))


@uip_bp.context_processor
def help_context():
    endpoint=(request.endpoint or "").split(".")[-1]
    area="command-centre"
    if "finance" in endpoint: area="finance"
    elif any(x in endpoint for x in ("survey","voting","meeting","decision","document")): area="governance"
    elif any(x in endpoint for x in ("member","property","register")): area="residents-properties"
    elif any(x in endpoint for x in ("provider","work_order","routing","sla","service_standards")): area="service-providers"
    elif any(x in endpoint for x in ("interaction","reception","task","municipal","communication","referral")): area="operations"
    elif "report" in endpoint: area="reports"
    elif any(x in endpoint for x in ("ai_","audit","settings","setup")): area="administration"
    return dict(uip_help_anchor=area)
