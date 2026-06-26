from app.models.auth import AuthSubject as Subject
from app.models.auth import AuthSubject as Subject
from app.extensions import db

def test_bridge_dashboard_redirects_unauthenticated(client):
    response = client.get('/bridge')
    # Should redirect to login
    assert response.status_code == 302
    assert '/login' in response.headers.get('Location', '')
