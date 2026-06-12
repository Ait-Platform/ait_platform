from app import create_app

app = create_app()
app.config['TESTING'] = True
client = app.test_client()

with app.app_context():
    from app.models.auth import User
    from flask_login import login_user
    
    @app.route('/auto_login')
    def auto_login():
        u = User.query.first()
        login_user(u)
        return "ok"
    
    client.get('/auto_login')
    resp = client.get('/billing/property/8/input_readings')
    html = resp.get_data(as_text=True)
    if 'value="49546"' in html or 'value="49546.0"' in html:
        print("Found the value in input readings!")
    else:
        print("Did not find the old reading 49546 in the HTML")
