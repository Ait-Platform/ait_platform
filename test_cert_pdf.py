from app import create_app
from app.subject_reading.routes import _generate_certificate_pdf
from datetime import datetime

app = create_app()

with app.app_context():
    pdf_path = _generate_certificate_pdf("TEST-CERT", "Sanjith", datetime.utcnow())
    print("PDF generated at:", pdf_path)
