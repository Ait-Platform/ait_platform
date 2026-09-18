with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace the GET Stats and POST logic in onboarding_campaign route
old_route = """@uip_bp.route("/<org_slug>/secretary/onboarding-campaign", methods=["GET", "POST"])
@login_required
def onboarding_campaign(org_slug):
    org = g.organization
    _require_secretary()
    
    from app.models.uip import UipMemberProfile, UipResolution
    from datetime import datetime, timedelta
    
    if request.method == "POST":
        wave = request.form.get("wave")
        # In a real app, this would trigger a background celery task to send emails.
        # For now, we just update the database state to simulate the blast.
        now = datetime.utcnow()
        if wave == "1":
            targets = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified", invite_wave=0).all()
            for t in targets:
                t.invite_wave = 1
                t.last_invite_at = now
            flash(f"Wave 1 dispatched to {len(targets)} ratepayers.", "success")
        elif wave == "2":
            cutoff = now - timedelta(days=3)
            targets = UipMemberProfile.query.filter(UipMemberProfile.organization_id==org.id, UipMemberProfile.eligibility_status=="unverified", UipMemberProfile.invite_wave==1, UipMemberProfile.last_invite_at < cutoff).all()
            for t in targets:
                t.invite_wave = 2
                t.last_invite_at = now
            flash(f"Wave 2 dispatched to {len(targets)} ratepayers.", "success")
        elif wave == "3":
            cutoff = now - timedelta(days=7)
            targets = UipMemberProfile.query.filter(UipMemberProfile.organization_id==org.id, UipMemberProfile.eligibility_status=="unverified", UipMemberProfile.invite_wave==2, UipMemberProfile.last_invite_at < cutoff).all()
            for t in targets:
                t.invite_wave = 3
                t.last_invite_at = now
            flash(f"Wave 3 dispatched to {len(targets)} ratepayers. Escalation hit list updated.", "success")
        
        db.session.commit()
        return redirect(url_for("uip_bp.onboarding_campaign", org_slug=org.slug))
        
    # GET Stats
    total_vault = UipMemberProfile.query.filter_by(organization_id=org.id).count() or 1
    active_members = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="eligible").count()
    activation_pct = int((active_members / total_vault) * 100)
    
    now = datetime.utcnow()
    wave_1_count = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified", invite_wave=0).count()
    wave_2_count = UipMemberProfile.query.filter(UipMemberProfile.organization_id==org.id, UipMemberProfile.eligibility_status=="unverified", UipMemberProfile.invite_wave==1, UipMemberProfile.last_invite_at < (now - timedelta(days=3))).count()
    wave_3_count = UipMemberProfile.query.filter(UipMemberProfile.organization_id==org.id, UipMemberProfile.eligibility_status=="unverified", UipMemberProfile.invite_wave==2, UipMemberProfile.last_invite_at < (now - timedelta(days=7))).count()
    
    recent_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").order_by(UipResolution.created_at.desc()).limit(3).all()
    
    return render_template(
        "program_uip/dashboards/onboarding_campaign.html",
        org=org,
        quorum_target=50,
        activation_pct=activation_pct,
        active_members=active_members,
        wave_1_count=wave_1_count,
        wave_2_count=wave_2_count,
        wave_3_count=wave_3_count,
        recent_resolutions=recent_resolutions
    )"""

new_route = """@uip_bp.route("/<org_slug>/secretary/onboarding-campaign", methods=["GET", "POST"])
@login_required
def onboarding_campaign(org_slug):
    org = g.organization
    _require_secretary()
    
    from app.models.uip import UipMemberProfile, UipResolution, UipMemberCampaign
    from datetime import datetime, timedelta
    
    # Helper to get campaign state
    def get_campaign(profile):
        if not profile.campaign_status:
            c = UipMemberCampaign(member_profile_id=profile.id, invite_wave=0)
            db.session.add(c)
            # flush so we can use it
            db.session.flush()
        return profile.campaign_status

    if request.method == "POST":
        wave = request.form.get("wave")
        now = datetime.utcnow()
        unverified = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified").all()
        count = 0
        
        for profile in unverified:
            camp = get_campaign(profile)
            if wave == "1" and camp.invite_wave == 0:
                camp.invite_wave = 1
                camp.last_invite_at = now
                count += 1
            elif wave == "2" and camp.invite_wave == 1 and camp.last_invite_at < (now - timedelta(days=3)):
                camp.invite_wave = 2
                camp.last_invite_at = now
                count += 1
            elif wave == "3" and camp.invite_wave == 2 and camp.last_invite_at < (now - timedelta(days=7)):
                camp.invite_wave = 3
                camp.last_invite_at = now
                count += 1
                
        flash(f"Wave {wave} dispatched to {count} ratepayers.", "success")
        db.session.commit()
        return redirect(url_for("uip_bp.onboarding_campaign", org_slug=org.slug))
        
    # GET Stats
    total_vault = UipMemberProfile.query.filter_by(organization_id=org.id).count() or 1
    active_members = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="eligible").count()
    activation_pct = int((active_members / total_vault) * 100)
    
    now = datetime.utcnow()
    unverified = UipMemberProfile.query.filter_by(organization_id=org.id, eligibility_status="unverified").all()
    
    wave_1_count = 0
    wave_2_count = 0
    wave_3_count = 0
    
    for profile in unverified:
        camp = get_campaign(profile)
        if camp.invite_wave == 0:
            wave_1_count += 1
        elif camp.invite_wave == 1 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=3)):
            wave_2_count += 1
        elif camp.invite_wave == 2 and camp.last_invite_at and camp.last_invite_at < (now - timedelta(days=7)):
            wave_3_count += 1
            
    db.session.commit() # save any newly created campaign rows
    
    recent_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="ADOPTED").order_by(UipResolution.created_at.desc()).limit(3).all()
    
    return render_template(
        "program_uip/dashboards/onboarding_campaign.html",
        org=org,
        quorum_target=50,
        activation_pct=activation_pct,
        active_members=active_members,
        wave_1_count=wave_1_count,
        wave_2_count=wave_2_count,
        wave_3_count=wave_3_count,
        recent_resolutions=recent_resolutions
    )"""

text = text.replace(old_route, new_route)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated onboarding_campaign route")
