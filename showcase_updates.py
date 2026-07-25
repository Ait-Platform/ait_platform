    shows = CfiShow.query.filter(CfiShow.is_private == False).all()
    
    # Get private shows for this user
    private_shows = []
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id).first()
    if enrollment:
        memberships = CfiGroupMember.query.filter_by(enrollment_id=enrollment.id).all()
        for member in memberships:
            if member.group.show_id:
                pshow = CfiShow.query.get(member.group.show_id)
                if pshow and pshow.is_private:
                    private_shows.append(pshow)
