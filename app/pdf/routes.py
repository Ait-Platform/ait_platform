from flask import Blueprint, Response, current_app, request, send_file
import sys, io
import pdfkit

pdf_bp = Blueprint("pdf_bp", __name__)

def _find_wkhtml():
    # implement logic to find wkhtmltopdf binary on Windows
    return "C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe"

def html_to_pdf_bytes(html: str, base_url: str | None = None) -> bytes:
    # Try WeasyPrint on non-Windows
    if sys.platform != "win32":
        try:
            from weasyprint import HTML
            return HTML(string=html, base_url=base_url).write_pdf()
        except Exception as e:
            current_app.logger.warning(f"WeasyPrint failed: {e}")

    # Try wkhtmltopdf
    exe = _find_wkhtml()
    if exe:
        try:
            import os
            if os.path.exists(exe):
                cfg = pdfkit.configuration(wkhtmltopdf=exe)
                options = {
                    "encoding": "UTF-8",
                    "enable-local-file-access": None,
                    "print-media-type": None,
                    "quiet": None,
                }
                return pdfkit.from_string(html, False, configuration=cfg, options=options)
        except Exception as e:
            current_app.logger.warning(f"wkhtmltopdf failed: {e}")

    # Fallback: CairoSVG
    try:
        import cairosvg
        return cairosvg.svg2pdf(bytestring=html.encode("utf-8"))
    except Exception as e:
        current_app.logger.exception(f"CairoSVG failed: {e}")
        raise RuntimeError("PDF generation not available: no backend succeeded.")

@pdf_bp.route("/test-pdf")
def test_pdf():
    html = "<h1>Hello PDF</h1><p>This is a test.</p>"
    pdf_bytes = html_to_pdf_bytes(html, base_url=request.host_url)
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="test.pdf",
        max_age=0,
    )



