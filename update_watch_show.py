import re

with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update watch_show
def replace_watch_show(text):
    old_pageant = '''    if show.category_item and show.category_item.name == "Pageant":
        submissions = (CfiSegmentItem.query
                       .filter_by(show_id=show.id)
                       .all())
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.title or "Untitled",
                "segment_type": sub.segment_type,
                "src": url_for("cultural_bp.uploaded_file", filename=sub.video_url) if not sub.video_url.startswith('uploads/') else url_for("cultural_bp.uploaded_file", filename=sub.video_url.replace('uploads/', ''))
            }
            for sub in submissions if sub.video_url
        ]
    else:
        submissions = (
            CfiTalentSubmission.query
            .filter_by(show_id=show.id)
            .all()
        )
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.talent_name or sub.custom_talent or "Untitled",
                "segment_type": "all",
                "src": url_for("cultural_bp.uploaded_file", filename=file.filename)
            }
            for sub in submissions
            for file in (sub.files or [])
            if file and file.filename
        ]'''

    new_pageant = '''    if show.category_item and show.category_item.name == "Pageant":
        submissions = (CfiSegmentItem.query
                       .filter_by(show_id=show.id)
                       .options(db.joinedload(CfiSegmentItem.enrollment))
                       .all())
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.title or "Untitled",
                "segment_type": sub.segment_type,
                "src": url_for("cultural_bp.uploaded_file", filename=sub.video_url) if not sub.video_url.startswith('uploads/') else url_for("cultural_bp.uploaded_file", filename=sub.video_url.replace('uploads/', '')),
                "user_id": sub.enrollment.user_id if sub.enrollment else None
            }
            for sub in submissions if sub.video_url
        ]
    else:
        submissions = (
            CfiTalentSubmission.query
            .filter_by(show_id=show.id)
            .all()
        )
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.talent_name or sub.custom_talent or "Untitled",
                "segment_type": "all",
                "src": url_for("cultural_bp.uploaded_file", filename=file.filename),
                "user_id": sub.user_id
            }
            for sub in submissions
            for file in (sub.files or [])
            if file and file.filename
        ]'''
    return text.replace(old_pageant, new_pageant)

# 2. Update submit_score API to prevent scoring own item
def replace_submit_score(text):
    start_idx = text.find('@cultural_bp.route("/api/judge/score/<int:show_id>", methods=["POST"])')
    end_idx = text.find('def get_scores(', start_idx)
    
    old_func = text[start_idx:end_idx]
    
    # Let's just find the part where it checks for existing score and add a check for own submission
    new_func = old_func.replace(
        '''    if show.category_item and show.category_item.name == "Pageant":
        submission = CfiSegmentItem.query.get_or_404(submission_id)
    else:
        submission = CfiTalentSubmission.query.get_or_404(submission_id)''',
        '''    if show.category_item and show.category_item.name == "Pageant":
        submission = CfiSegmentItem.query.get_or_404(submission_id)
        sub_user_id = submission.enrollment.user_id if submission.enrollment else None
    else:
        submission = CfiTalentSubmission.query.get_or_404(submission_id)
        sub_user_id = submission.user_id
        
    if sub_user_id == current_user.id:
        return jsonify({"success": False, "message": "You cannot score your own submission."}), 403'''
    )
    
    return text.replace(old_func, new_func)

text = replace_watch_show(text)
text = replace_submit_score(text)

with open('app/program_culturalfire/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated watch_show and submit_score in routes.py")
