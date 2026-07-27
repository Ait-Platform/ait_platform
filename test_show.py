from app import create_app, db
from app.models.culturalfire import CfiShow, CfiPrivateShowGroup
from app.models.auth import UserEnrollment

app = create_app()
with app.app_context():
    show = CfiShow.query.filter(CfiShow.title.ilike('%the smith family Private%')).first()
    if show:
        print(f"Found show: {show.id}")
        psg = CfiPrivateShowGroup.query.filter_by(show_id=show.id).first()
        if psg:
            print(f"Found psg for group {psg.group.id}")
            for member in psg.group.group_members:
                print(f"Member: {member.id}, Enrollment: {member.enrollment}")
                if member.enrollment:
                    print(f"  Country Code: {member.enrollment.country_code}")
                    if member.enrollment.biodata:
                        print(f"  Gender: {member.enrollment.biodata.gender}")
                    else:
                        print("  Biodata is missing")
    else:
        print("Show not found.")
