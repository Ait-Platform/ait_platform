from app import create_app
from flask import render_template

app = create_app()

with app.test_request_context('/billing/setup'):
    try:
        render_template('school_billing/setup_wizard.html')
        print("Template rendered successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()
