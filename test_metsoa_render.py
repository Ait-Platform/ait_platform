import sys
from app import create_app, db
from flask import render_template
from app.school_billing.routes import metsoa

app = create_app()

with app.app_context():
    app.config['SERVER_NAME'] = 'localhost'
    with app.test_request_context('/billing/metsoa/3/2026-05'):
        response = metsoa(3, "2026-05")
        with open("metsoa_output.html", "w", encoding="utf-8") as f:
            f.write(response)
        print("Template rendered successfully.")
