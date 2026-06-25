import sys, json

# Add the project root to PYTHONPATH
sys.path.append('D:/Users/yeshk/Documents/ait_platform')

from app import create_app

app = create_app()

with app.app_context():
    from app.models.auth import AuthSubject
    subjects = AuthSubject.query.all()
    data = []
    for s in subjects:
        data.append({
            'slug': s.slug,
            'name': s.name,
            'start_endpoint': s.start_endpoint,
            'about_endpoint': s.about_endpoint,
            'pay_endpoint': s.pay_endpoint,
            'admin_start_endpoint': s.admin_start_endpoint,
            'is_hidden_on_bridge': s.is_hidden_on_bridge,
        })
    print(json.dumps(data, indent=2))
