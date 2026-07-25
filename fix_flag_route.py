with open('app/program_culturalfire/routes.py', 'r') as f:
    content = f.read()

flag_route = '''
@cultural_bp.route("/flag_video/<video_id>", methods=["POST"])
@login_required
def flag_video(video_id):
    from app.models.culturalfire import CfiVideoFlag
    
    # Check if user already flagged
    existing_flag = CfiVideoFlag.query.filter_by(video_id=video_id, reporter_id=current_user.id).first()
    if existing_flag:
        return jsonify({"success": False, "message": "You have already flagged this video."})
        
    new_flag = CfiVideoFlag(video_id=video_id, reporter_id=current_user.id)
    db.session.add(new_flag)
    db.session.commit()
    
    # Check flag count
    flag_count = CfiVideoFlag.query.filter_by(video_id=video_id).count()
    if flag_count >= 3:
        # It will be hidden on next load
        pass
        
    return jsonify({"success": True, "message": "Video flagged for review. Thank you for keeping the community safe."})
'''

content = content + "\n" + flag_route + "\n"

with open('app/program_culturalfire/routes.py', 'w') as f:
    f.write(content)
print("Done")
