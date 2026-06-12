import os
import pdfkit
from flask import Flask, render_template_string

app = Flask(__name__)

with app.app_context():
    html = "<h1>Test</h1>"
    exe = os.getenv("WKHTMLTOPDF_EXE", r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    try:
        cfg = pdfkit.configuration(wkhtmltopdf=exe)
        options = {"encoding": "UTF-8", "enable-local-file-access": None, "print-media-type": None, "quiet": None}
        pdf_bytes = pdfkit.from_string(html, False, configuration=cfg, options=options)
        print("PDF generated successfully, size:", len(pdf_bytes))
    except Exception as e:
        print("PDF generation failed:", str(e))
