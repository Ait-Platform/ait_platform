import requests
import re
from app import create_app
from app.extensions import db
from app.models.auth import User, AuthSubject
from app.models.auth import UserEnrollment
from app.models.loss import LcaRun

app = create_app()

def test_flow():
    with app.test_client() as client:
        with app.app_context():
            # 1. Create dummy user
            email = 'test_loss_paid@example.com'
            u = User.query.filter_by(email=email).first()
            if not u:
                u = User(email=email, is_active=1)
                db.session.add(u)
                db.session.commit()
            uid = u.id

            # 2. Get subject
            s = AuthSubject.query.filter_by(slug='loss').first()
            sid = s.id

            # 3. Create active enrollment
            enr = UserEnrollment.query.filter_by(user_id=uid, subject_id=sid).first()
            if not enr:
                enr = UserEnrollment(user_id=uid, subject_id=sid, status='active')
                db.session.add(enr)
                db.session.commit()
            else:
                enr.status = 'active'
                db.session.commit()

        # 4. Login
        with client.session_transaction() as sess:
            sess['_user_id'] = str(uid)
            sess['email'] = email
            sess['is_authenticated'] = True

        # 5. Hit subject_home
        print("--- HITTING /loss/home ---")
        res = client.get('/loss/home', follow_redirects=False)
        print("Status:", res.status_code)
        if res.status_code in (301, 302):
            print("Redirect:", res.headers.get('Location'))

        # 6. Hit course_start
        print("\n--- HITTING /loss/course/start ---")
        res = client.get('/loss/course/start', follow_redirects=False)
        print("Status:", res.status_code)
        if res.status_code in (301, 302):
            print("Redirect:", res.headers.get('Location'))
            next_url = res.headers.get('Location')
        else:
            print("No redirect from course/start!")
            return

        # 7. Hit assessment_question_flow(from_pos=1)
        print(f"\n--- HITTING {next_url} (Card 1) ---")
        res = client.get(next_url, follow_redirects=False)
        print("Status:", res.status_code)
        if res.status_code in (301, 302):
            print("Redirect:", res.headers.get('Location'))
            return
        
        # find the next_url from the button
        html = res.data.decode()
        match = re.search(r'href="(/loss/assessment_question_flow[^"]+)"', html)
        if match:
            next_card_url = match.group(1).replace('&amp;', '&')
            print("Next button points to:", next_card_url)
        else:
            print("COULD NOT FIND NEXT BUTTON URL")
            print(html[:500])
            return

        # 8. Hit assessment_question_flow(from_pos=2)
        print(f"\n--- HITTING {next_card_url} (Card 2) ---")
        res = client.get(next_card_url, follow_redirects=False)
        print("Status:", res.status_code)
        if res.status_code in (301, 302):
            print("Redirect:", res.headers.get('Location'))
        else:
            print("Rendered successfully.")

if __name__ == '__main__':
    test_flow()
