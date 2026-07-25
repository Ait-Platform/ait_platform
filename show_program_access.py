    if show.is_private:
        # Check if unlocked
        access = CfiShowAccess.query.filter_by(user_id=current_user.id, show_id=show.id).first()
        if not access:
            flash("You must unlock this private show first.", "danger")
            return redirect(url_for('cultural_bp.showcase_dashboard'))
