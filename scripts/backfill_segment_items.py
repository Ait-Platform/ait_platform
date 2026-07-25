import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from app import create_app
from app.extensions import db
from app.models.culturalfire import CfiTalentSubmission, CfiSegmentItem

app = create_app()

with app.app_context():
    # Find all pageant submissions
    submissions = CfiTalentSubmission.query.filter(CfiTalentSubmission.talent_name.isnot(None)).all()
    count = 0
    for sub in submissions:
        # Check if it corresponds to a pageant segment
        # e.g. "Ramp Walk", "Intro", "Talent", "Traditional Wear", "Formal Wear", "Q&A"
        if sub.talent_name in ["Ramp Walk", "Intro", "Talent", "Traditional Wear", "Formal Wear", "Q&A"]:
            segment_type_norm = sub.talent_name.lower().replace(" ", "_").replace("&", "n")
            
            # Check if segment item already exists
            seg_item = CfiSegmentItem.query.filter_by(
                enrollment_id=sub.user_enrollment_id,
                show_id=sub.show_id,
                segment_type=segment_type_norm
            ).first()
            
            if not seg_item:
                seg_item = CfiSegmentItem(
                    enrollment_id=sub.user_enrollment_id,
                    show_id=sub.show_id,
                    segment_type=segment_type_norm,
                    title=sub.talent_name,
                    status=sub.status,
                    video_url=sub.video_url
                )
                db.session.add(seg_item)
                count += 1
                
    db.session.commit()
    print(f"Backfilled {count} CfiSegmentItems.")
