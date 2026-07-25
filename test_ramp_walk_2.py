import traceback
from app import create_app, db
from app.models.auth import User, UserEnrollment, AuthSubject
from app.models.culturalfire import CfiTalentCategoryItem, CfiShow, CfiPageantSegment
from io import BytesIO

app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False

with app.test_client() as client:
    with app.app_context():
        # Find a user who has an active enrollment in cultural fire
        cfi_subject = AuthSubject.query.filter_by(slug='cultural_fire').first()
        if not cfi_subject:
            print('No cultural fire subject')
            exit(1)
        enr = UserEnrollment.query.filter_by(subject_id=cfi_subject.id).first()
        if not enr:
            print('No enrollment')
            exit(1)
        user = User.query.get(enr.user_id)
        show = CfiShow.query.filter_by(status='active').first()
        if not show:
            print('No active show')
            exit(1)
        cat = show.category_item
        print(f'User: {user.email}, Enr: {enr.id}, Cat: {cat.id}, Show: {show.id}')
    
    # Login
    # To avoid needing password, we can simulate login by using Flask-Login's login_user in a view
    @app.route('/auto_login/<int:uid>')
    def auto_login(uid):
        from flask_login import login_user
        u = User.query.get(uid)
        login_user(u)
        return 'Logged in'
    
    client.get(f'/auto_login/{user.id}')
    
    # Post to ramp_walk
    data = {'video': (BytesIO(b'fake video data'), 'pag1.mp4')}
    try:
        url = f'/talent/pageant/{enr.id}/{cat.id}/ramp_walk'
        print(f'POSTing to {url}')
        r = client.post(url, data=data, content_type='multipart/form-data')
        print('Status:', r.status_code)
        if r.status_code == 500:
            print(r.data.decode()[:2000])
        elif r.status_code == 302:
            print('Redirected to:', r.headers.get('Location'))
    except Exception as e:
        print('EXCEPTION CAUGHT:')
        traceback.print_exc()

