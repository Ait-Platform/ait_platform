from app import create_app
from app.extensions import db
from app.models.auth import User
import traceback

app = create_app()

with app.app_context():
    try:
        from app.models.culturalfire import CfiShow, CfiSegmentItem
        from sqlalchemy.orm import joinedload
        from app.models.auth import UserEnrollment
        from app.program_culturalfire.helpers import calculate_age_from_dob
        from flask import url_for
        
        # Mock request context for url_for
        with app.test_request_context():
            shows = CfiShow.query.all()
            for show in shows:
                if show.category_item and show.category_item.name == "Pageant":
                    print(f"Testing watch_show logic for Pageant show_id: {show.id}")
                    
                    submissions = (CfiSegmentItem.query
                                   .filter_by(show_id=show.id)
                                   .options(db.joinedload(CfiSegmentItem.enrollment))
                                   .all())
                    
                    submissions_data = []
                    from app.models.culturalfire import CfiTalentSubmission, CfiPageantQuestion
                    
                    def get_url(url):
                        if not url: return ""
                        if url.startswith('/static/uploads/'): return "mock_url"
                        if url.startswith('cfi/'): return "mock_url"
                        if not url.startswith('http'): return "mock_url"
                        return url
                        
                    for sub in submissions:
                        if not sub.video_url:
                            continue
                            
                        question_text = ""
                        if sub.segment_type in ["Q&A", "qna", "q_and_a"]:
                            ts = CfiTalentSubmission.query.filter_by(
                                user_enrollment_id=sub.enrollment_id, 
                                show_id=show.id, 
                                talent_name="Q&A"
                            ).first()
                            if ts and ts.qna_question_id:
                                pq = CfiPageantQuestion.query.get(ts.qna_question_id)
                                if pq:
                                    question_text = pq.question
                                    
                        submissions_data.append({
                            "id": sub.id,
                            "title": sub.title or "Untitled",
                            "segment_type": sub.segment_type,
                            "src": get_url(sub.video_url),
                            "question_text": question_text,
                            "item_type": "act",
                            "has_voted": False,
                            "user_id": sub.enrollment.user_id if sub.enrollment else None
                        })
                    print(f"Successfully ran watch_show for Pageant {show.id}")
                    
    except Exception as e:
        print("ERROR OCCURRED:")
        traceback.print_exc()
