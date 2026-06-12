import re

with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Pass is_judge to watch_show
old_render_watch = """    return render_template(
        "program_culturefire/watch_show.html","""
new_render_watch = """    is_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first() is not None
    return render_template(
        "program_culturefire/watch_show.html",
        is_judge=is_judge,"""
if old_render_watch in content:
    content = content.replace(old_render_watch, new_render_watch)

vote_match = re.search(r'@cultural_bp\.route\("/show/vote", methods=\["POST"\]\).*?def vote_item\(\):.*?return jsonify.*?\}', content, re.DOTALL)
if vote_match:
    old_vote_item = vote_match.group(0)
    new_vote_item = """@cultural_bp.route("/show/vote", methods=["POST"])
@login_required
def vote_item():
    data = request.json
    sub_id = data.get("submission_id")
    v_type = data.get("type", "talent")
    score = int(data.get("score", 0))

    if v_type == "pageant":
        item = CfiSegmentItem.query.get(sub_id)
        if not item:
            return jsonify({"success": False, "message": "Segment item not found"})
        
        if not CfiJudgeAssignment.query.filter_by(show_id=item.show_id, judge_id=current_user.id).first():
            return jsonify({"success": False, "message": "Only assigned judges can score this pageant!"})

        existing_vote = CfiShowcaseVote.query.filter_by(user_id=current_user.id, segment_item_id=sub_id).first()
        if existing_vote:
            existing_vote.score = score
        else:
            vote = CfiShowcaseVote(user_id=current_user.id, segment_item_id=sub_id, score=score)
            db.session.add(vote)
    else:
        sub = CfiTalentSubmission.query.get(sub_id)
        if not sub:
            return jsonify({"success": False, "message": "Submission not found"})
            
        if not CfiJudgeAssignment.query.filter_by(show_id=sub.show_id, judge_id=current_user.id).first():
            return jsonify({"success": False, "message": "Only assigned judges can score this show!"})

        existing_vote = CfiShowcaseVote.query.filter_by(user_id=current_user.id, submission_id=sub_id).first()
        if existing_vote:
            existing_vote.score = score
        else:
            vote = CfiShowcaseVote(user_id=current_user.id, submission_id=sub_id, score=score)
            db.session.add(vote)

    db.session.commit()
    return jsonify({"success": True})"""
    content = content.replace(old_vote_item, new_vote_item)

with open(r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated show_program and vote_item successfully.")
