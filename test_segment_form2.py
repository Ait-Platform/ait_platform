
import traceback
from app import create_app
from app.models.auth import User, UserEnrollment
from app.models.culturalfire import CfiShow, CfiPageantSegment
from io import BytesIO
app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False
with app.app_context():
    show = CfiShow.query.filter_by(status='active').first()
    cat = show.category_item
    enr = UserEnrollment.query.filter_by(subject_id=12).first()
    user = User.query.get(enr.user_id)
    seg = CfiPageantSegment.query.filter_by(name='Ramp Walk').first()
url = f'/talent/pageant/segment_form/{enr.id}/{show.id}/{cat.id}?segment_id={seg.id}'
print(f'POST {url}')
with app.test_client() as client:
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user.id)
        sess['_fresh'] = True
    try:
        res = client.post(url, data={'video': (BytesIO(b'foo'), 'test.mp4')}, content_type='multipart/form-data')
        print(f'Status: {res.status_code}')
        if res.status_code == 500:
            print(res.text)
    except Exception:
        traceback.print_exc()

