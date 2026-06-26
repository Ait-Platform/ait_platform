from app.models.auth import User, AuthSubject as Subject
from app.extensions import db

def test_login_page_renders(client):
    response = client.get('/login')
    assert response.status_code == 200
    assert b'Sign In' in response.data

def test_register_page_renders(client):
    response = client.get('/register')
    assert response.status_code == 200
    assert b'Create your account' in response.data
