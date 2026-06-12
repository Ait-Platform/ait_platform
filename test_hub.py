from app import create_app
from app.models.auth import User
from bs4 import BeautifulSoup

app = create_app()
app.config['TESTING'] = True
client = app.test_client()

with app.app_context():
    from flask_login import login_user
    
    @app.route('/auto_login')
    def auto_login():
        u = User.query.first()
        u.role = 'admin' 
        login_user(u)
        return "ok"
    
    client.get('/auto_login')
    # Default property_hub URL
    resp = client.get('/billing/property/8/hub')
    html = resp.get_data(as_text=True)
    
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr')
    for row in rows:
        if 'AGN489' in row.text:
            print("Found AGN489 row!")
            # Print the text content of the row
            print(row.get_text(strip=True, separator=' | '))
