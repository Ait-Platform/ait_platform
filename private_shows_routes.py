
@cultural_bp.route("/private_show/dashboard/<int:enrollment_id>")
@login_required
def private_show_dashboard(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    if enrollment.user_id != current_user.id:
        abort(403)
        
    categories = CfiTalentCategoryItem.query.all()
        
    groups_led = CfiGroup.query.filter(CfiGroup.leader_id == enrollment.id, CfiGroup.show_id != None).all()
    memberships = CfiGroupMember.query.filter(CfiGroupMember.enrollment_id == enrollment.id, CfiGroupMember.submission_id == None).all()
    
    return render_template("program_culturefire/private_show_dashboard.html", 
                           enrollment=enrollment, 
                           groups_led=groups_led, 
                           memberships=memberships,
                           categories=categories)

@cultural_bp.route("/private_show/create/<int:enrollment_id>", methods=["POST"])
@login_required
def create_private_show(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    if enrollment.user_id != current_user.id:
        abort(403)
        
    title = request.form.get("title")
    category_item_id = request.form.get("category_item_id")
    
    if not title or not category_item_id:
        flash("Title and category are required.", "danger")
        return redirect(url_for('cultural_bp.private_show_dashboard', enrollment_id=enrollment_id))
        
    new_show = CfiShow(
        title=title,
        description="A private, exclusive show.",
        start_date=datetime.utcnow().date(),
        location="Virtual",
        category_item_id=category_item_id,
        status="active",
        is_private=True
    )
    db.session.add(new_show)
    db.session.flush() # To get the show.id
    
    group = CfiGroup(
        name=f"{title} Group",
        leader_id=enrollment.id,
        show_id=new_show.id
    )
    db.session.add(group)
    db.session.flush()
    
    # Add leader as member
    member = CfiGroupMember(
        group_id=group.id,
        enrollment_id=enrollment.id
    )
    db.session.add(member)
    db.session.commit()
    
    flash("Private show created! You can now invite members.", "success")
    return redirect(url_for('cultural_bp.private_show_dashboard', enrollment_id=enrollment_id))

@cultural_bp.route("/private_show/join/<int:group_id>", methods=["GET", "POST"])
@login_required
def join_private_show(group_id):
    group = CfiGroup.query.get_or_404(group_id)
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id).first()
    
    if not enrollment:
        flash("You must be enrolled to join a private show.", "danger")
        return redirect(url_for('hub_bp.hub_dashboard'))
        
    existing = CfiGroupMember.query.filter_by(group_id=group.id, enrollment_id=enrollment.id).first()
    if existing:
        flash("You are already a member of this private show.", "info")
    else:
        member = CfiGroupMember(
            group_id=group.id,
            enrollment_id=enrollment.id
        )
        db.session.add(member)
        db.session.commit()
        flash("Successfully joined the private show group!", "success")
        
    return redirect(url_for('cultural_bp.private_show_dashboard', enrollment_id=enrollment.id))

@cultural_bp.route("/private_show/unlock/<int:show_id>", methods=["POST"])
@login_required
def unlock_private_show(show_id):
    show = CfiShow.query.get_or_404(show_id)
    if not show.is_private:
        return jsonify({"success": False, "message": "This show is public."})
        
    # Check if they are a member
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id).first()
    if not enrollment:
        return jsonify({"success": False, "message": "You must be enrolled to view this show."})
        
    group = CfiGroup.query.filter_by(show_id=show.id).first()
    if not group:
        return jsonify({"success": False, "message": "Group not found."})
        
    is_member = CfiGroupMember.query.filter_by(group_id=group.id, enrollment_id=enrollment.id).first()
    if not is_member:
        return jsonify({"success": False, "message": "You are not a member of this exclusive group."})
        
    # Check if already unlocked
    existing_access = CfiShowAccess.query.filter_by(user_id=current_user.id, show_id=show.id).first()
    if existing_access:
        return jsonify({"success": True, "message": "Already unlocked."})
        
    # Token Logic
    tariff = CfiTokenTariff.query.filter_by(action_name='private_show_view').first()
    token_cost = tariff.base_token_cost if tariff else 10
    
    from app.models.debtors import DebtorsWallet, DebtorsTokenTransaction
    wallet = DebtorsWallet.query.filter_by(user_id=current_user.id).first()
    if not wallet or wallet.token_balance < token_cost:
        return jsonify({"success": False, "message": f"Insufficient tokens. You need {token_cost} tokens."})
        
    wallet.token_balance -= token_cost
    txn = DebtorsTokenTransaction(
        wallet_id=wallet.id,
        amount=-token_cost,
        description=f"Unlocked private show: {show.title}"
    )
    db.session.add(txn)
    
    access = CfiShowAccess(
        user_id=current_user.id,
        show_id=show.id,
        tokens_paid=token_cost
    )
    db.session.add(access)
    db.session.commit()
    
    return jsonify({"success": True, "message": "Show unlocked successfully!"})
