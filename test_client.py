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
    resp = client.get('/metsoa/3/2026-05')
    html = resp.get_data(as_text=True)
    if '7150696S' in html:
        lines = html.split('\n')
        for i, line in enumerate(lines):
            if '7150696S' in line:
                for j in range(max(0, i-5), min(len(lines), i+15)):
                    print(lines[j])
                break
    else:
        print("Not found in HTML")
