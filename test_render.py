from app import create_app, db
from app.models.culturalfire import CfiShow, CfiPrivateShowGroup, CfiTalentCategoryItem, CfiMcAssignment, CfiJudgeAssignment, CfiShowAd
from app.models.auth import User
from flask import render_template

app = create_app()
with app.app_context():
    # Simulate current_user via a mock
    class MockUser:
        id = 1
    
    app.jinja_env.globals['current_user'] = MockUser()
    
    show = CfiShow.query.filter(CfiShow.title.ilike('%the smith family Private%')).first()
    if not show:
        # Just grab any private show
        psg = CfiPrivateShowGroup.query.first()
        show = CfiShow.query.get(psg.show_id)
        
    print(f"Testing with show: {show.title}")
    
    is_private_show = True
    submissions = []
    psg = CfiPrivateShowGroup.query.filter_by(show_id=show.id).first()
    if psg and psg.group:
        class MockSub:
            pass
        for member in psg.group.group_members:
            sub = MockSub()
            sub.id = member.id
            sub.user_enrollment = member.enrollment
            sub.talent_name = "Private Show Member"
            sub.custom_talent = None
            sub.group_members = []
            sub.sponsors = []
            sub.supporters = []
            submissions.append(sub)

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
            from app.program_culturalfire.helpers import calculate_age_from_dob
            sub.user_enrollment.biodata.age_calc = calculate_age_from_dob(sub.user_enrollment.biodata.dob)

    print(f"Mocked {len(submissions)} submissions.")
    
    try:
        # Minimal context for rendering
        ctx = {
            'show': show,
            'submissions': submissions,
            'submissions_by_segment': None,
            'origin': 'test',
            'enrollment_id': 1,
            'show_mcs': [],
            'show_judges': [],
            'show_advertisers': []
        }
        with app.test_request_context('/'):
            html = render_template("program_culturefire/program.html", **ctx)
            print("Rendered successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()
