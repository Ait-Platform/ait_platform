    # Get private shows for this user
    private_shows = []
    unlocked_private_show_ids = []
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id).first()
    if enrollment:
        memberships = CfiGroupMember.query.filter_by(enrollment_id=enrollment.id).all()
        for member in memberships:
            if member.group.show_id:
                pshow = CfiShow.query.get(member.group.show_id)
                if pshow and pshow.is_private:
                    private_shows.append(pshow)
                    
        # Check unlocked
        accesses = CfiShowAccess.query.filter_by(user_id=current_user.id).all()
        unlocked_private_show_ids = [a.show_id for a in accesses]
