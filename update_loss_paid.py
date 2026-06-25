import sys

# Add project root to sys.path
sys.path.append('D:/Users/yeshk/Documents/ait_platform')

from app import create_app, db

app = create_app()

with app.app_context():
    from app.models.auth import AuthSubject
    # Fetch the loss subject
    loss = AuthSubject.query.filter_by(slug='loss').first()
    if not loss:
        raise RuntimeError('Loss subject not found')
    # Set to paid mode, ensure pricing required, no trial days
    loss.commercial_mode = 'paid'
    loss.requires_price = 1
    loss.trial_days = 0
    # Ensure standard endpoints are set (adjust if needed)
    loss.start_endpoint = 'loss_bp.subject_home'  # Welcome/Explore
    loss.about_endpoint = 'loss_bp.about_loss'   # About page
    loss.pay_endpoint = 'loss_bp.enrol_loss'    # Registration/payment entry point
    loss.admin_start_endpoint = 'admin_bp.loss_dashboard'  # Admin dashboard if needed
    # Commit changes
    db.session.commit()
    print('Loss subject updated to paid and standard flow')
