"""Isolated rendering regression; no database connections or writes."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from flask import Flask, g, render_template_string
from flask_login import LoginManager
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=True)
from app.extensions import db
from app.program_uip import uip_bp, establish_menu_context, menu_context

app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[2] / 'templates'))
app.config.update(TESTING=True, SECRET_KEY='test-only', SQLALCHEMY_DATABASE_URI='postgresql://unused:unused@localhost/unused')
db.init_app(app)
LoginManager(app)
app.register_blueprint(uip_bp)
app.add_url_rule('/', endpoint='public_bp.welcome', view_func=lambda: '')
app.add_url_rule('/logout', endpoint='auth_bp.logout', view_func=lambda: '')
app.jinja_env.globals['csrf_token'] = lambda: 'test-token'
org = SimpleNamespace(id=1, slug='repair-test', name='Repair Test')
user = SimpleNamespace(id=1, email='secretary@example.invalid', is_authenticated=True, is_active=True, is_anonymous=False)
@app.before_request
def fixtures():
    g.organization = org
    g._login_user = user
# Isolate organization/billing data setup, retain the real shared menu hook.
app.before_request_funcs['uip_bp'] = [establish_menu_context]
query = MagicMock()
query.filter.return_value = query
query.filter_by.return_value = query
query.order_by.return_value = query
query.all.return_value = []
query.first.return_value = SimpleNamespace(position='Secretary', is_active=True)
with app.app_context(), patch.object(db.Model, 'query', query), patch.object(db.session, 'commit'):
    with app.test_request_context('/uip/repair-test/router'):
        assert menu_context()['force_menu'] is False
    client = app.test_client()
    for suffix, expected in [('', False), ('?force=1', True), ('?force=', False)]:
        with app.test_request_context('/uip/repair-test/router' + suffix):
            fixtures()
            establish_menu_context()
            assert type(g.force_menu) is bool and g.force_menu is expected
            assert menu_context()['force_menu'] is expected
            assert render_template_string('{{ force_menu }}', force_menu=True) == 'True'
        response = client.get('/uip/repair-test/secretary-workspace' + suffix)
        assert response.status_code == 200, response.status_code
        assert b'Verification of Members' in response.data
        assert b'Back to Committee Board' in response.data
        print('PASS Secretary workspace + navigation', suffix or '(default)')
    response = client.get('/uip/repair-test/router')
    assert response.status_code == 302 and 'committee' in response.location
    response = client.get('/uip/repair-test/router?force=1')
    assert response.status_code == 200
    response = client.get('/uip/repair-test/waiting-lounge')
    assert response.status_code == 302 and 'committee' in response.location
    response = client.get('/uip/repair-test/verify/committee')
    assert response.status_code == 302 and 'committee' in response.location
    print('PASS committee verification redirect')
    print('PASS router default redirect, forced menu render, waiting-lounge verified redirect')
print('PASS boolean defaults and explicit True template override')
