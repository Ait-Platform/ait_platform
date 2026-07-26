
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
        shows = CfiShow.query.filter_by(status='active').all()
        target_show = None
        for s in shows:
            cat = s.category_item
            if cat:
                target_show = s
                break
                
        if not target_show:
            print('No active show with a category')
            exit(1)
            
        cat = target_show.category_item
        enr = UserEnrollment.query.filter_by(subject_id=12).first()
        if not enr:
            print('No enrollment')
            exit(1)
            
        user = User.query.get(enr.user_id)
        
        print(f'User: {user.email}, Enr: {enr.id}, Cat: {cat.id}, Show: {target_show.id}')
        
    @app.route('/auto_login/<int:uid>')
    def auto_login(uid):
        from flask_login import login_user
        u = User.query.get(uid)
        login_user(u)
        return 'Logged in'
        
    client.get(f'/auto_login/{user.id}')
    
    data = {'video': (BytesIO(b'fake video data'), 'pag1.mp4')}
    url = f'/talent/pageant/{enr.id}/{cat.id}/ramp_walk'
    print(f'POSTing to {url}')
    
    try:
        r = client.post(url, data=data, content_type='multipart/form-data')
        print('Status:', r.status_code)
        if r.status_code == 500:
            print('--- 500 ERROR CAUGHT ---')
            print(r.data.decode()[:2000])
        elif r.status_code == 302:
            print('Redirected to:', r.headers.get('Location'))
    except Exception as e:
        print('EXCEPTION CAUGHT:')
        traceback.print_exc()

