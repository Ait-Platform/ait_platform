from wsgi import app
from app.utils.mailer import send_email

with app.app_context():
    print("MAIL_SERVER:", app.config.get("MAIL_SERVER"))
    print("MAIL_PORT:", app.config.get("MAIL_PORT"))
    print("MAIL_USE_TLS:", app.config.get("MAIL_USE_TLS"))
    print("MAIL_USERNAME:", app.config.get("MAIL_USERNAME"))
    print("MAIL_SUPPRESS_SEND:", app.config.get("MAIL_SUPPRESS_SEND"))
    
    print("Sending test email...")
    ok = send_email(
        "Test email from local environment",
        ["spv@gmail.com"],
        body="This is a test email to verify SMTP configuration.",
    )
    print("Result of send_email:", ok)
