from app.models.auth import User, Subject
from app.extensions import db

def test_login_page_renders(client):
    response = client.get('/auth/login')
    assert response.status_code == 200
    assert b'Log In' in response.data

def test_register_page_renders(client):
    response = client.get('/auth/register')
    assert response.status_code == 200
    assert b'Register' in response.data
