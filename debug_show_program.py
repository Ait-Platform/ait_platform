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
        
        shows = CfiShow.query.all()
        for show in shows:
            if show.category_item and show.category_item.name == "Pageant":
                print(f"Testing Pageant show_id: {show.id}")
                segment_items = (CfiSegmentItem.query
                               .filter_by(show_id=show.id)
                               .options(joinedload(CfiSegmentItem.enrollment)
                                        .joinedload(UserEnrollment.biodata))
                               .all())
                
                submissions_by_segment = {}
                from app.models.culturalfire import CfiBiodata
                for item in segment_items:
                    seg = item.segment_type.replace('_', ' ').title()
                    if seg not in submissions_by_segment:
                        submissions_by_segment[seg] = []
                    
                    item.user_enrollment = item.enrollment
                    item.talent_name = item.title
                    
                    if item.user_enrollment and not item.user_enrollment.biodata:
                        item.user_enrollment.biodata = CfiBiodata.query.filter_by(user_id=item.user_enrollment.user_id).first()
                        
                    if item.user_enrollment and item.user_enrollment.biodata and item.user_enrollment.biodata.dob:
                        item.user_enrollment.biodata.age_calc = calculate_age_from_dob(item.user_enrollment.biodata.dob)
                        
                    submissions_by_segment[seg].append(item)
                    
                print(f"Successfully grouped segment items for {show.id}")
                
            else:
                from app.models.culturalfire import CfiTalentSubmission
                submissions = (CfiTalentSubmission.query
                               .filter_by(show_id=show.id)
                               .options(joinedload(CfiTalentSubmission.user_enrollment)
                                        .joinedload(UserEnrollment.biodata))
                               .all())
                
                from app.models.culturalfire import CfiBiodata
                for sub in submissions:
                    if not hasattr(sub, 'user_enrollment'):
                        sub.user_enrollment = getattr(sub, 'enrollment', None)
                    if not hasattr(sub, 'talent_name'):
                        sub.talent_name = getattr(sub, 'title', None)
                    if not hasattr(sub, 'custom_talent'):
                        sub.custom_talent = None
                    if not hasattr(sub, 'group_members'):
                        sub.group_members = []
                    if not hasattr(sub, 'sponsors'):
                        sub.sponsors = []
                    if not hasattr(sub, 'supporters'):
                        sub.supporters = []

                    if sub.user_enrollment and not sub.user_enrollment.biodata:
                        sub.user_enrollment.biodata = CfiBiodata.query.filter_by(user_id=sub.user_enrollment.user_id).first()

                    if sub.user_enrollment and sub.user_enrollment.biodata and sub.user_enrollment.biodata.dob:
                        sub.user_enrollment.biodata.age_calc = calculate_age_from_dob(sub.user_enrollment.biodata.dob)

                print(f"Successfully processed normal show {show.id}")
                
    except Exception as e:
        print("ERROR OCCURRED:")
        traceback.print_exc()
