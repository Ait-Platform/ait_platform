from app import create_app
from app.models.auth import User

app = create_app()
app.config['TESTING'] = True
client = app.test_client()

with app.app_context():
    from flask_login import login_user
    
    @app.route('/auto_login')
    def auto_login():
        u = User.query.first()
        # Mock role attribute for testing
        u.role = 'admin' 
        login_user(u)
        return "ok"
    
    client.get('/auto_login')
    resp = client.get('/billing/property/8/input_readings?date=2026-05-29')
    html = resp.get_data(as_text=True)
    if 'AGN489' in html:
        lines = html.split('\n')
        for i, line in enumerate(lines):
            if 'AGN489' in line:
                for j in range(i, min(i+30, len(lines))):
                    if 'value=' in lines[j] and '14889' in lines[j]:
                        print("SUCCESS: 14889 found in input value")
                        print(lines[j])
                        break
                else:
                    print("AGN489 found but 14889 value NOT found. Printing lines:")
                    for j in range(i, min(i+30, len(lines))):
                        print(lines[j])
                break
        print("Done")
    else:
        print("AGN489 Not found in HTML")
