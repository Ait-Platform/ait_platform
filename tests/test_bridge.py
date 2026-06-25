from app.models.auth import Subject
from app.extensions import db

def test_welcome_page_renders(client):
    response = client.get('/bridge/welcome')
    assert response.status_code == 200
    assert b'Welcome to Bridge' in response.data or b'Bridge' in response.data

def test_bridge_dashboard_requires_login(client):
    response = client.get('/bridge/dashboard')
    assert response.status_code == 302
    assert '/auth/login' in response.headers['Location']
