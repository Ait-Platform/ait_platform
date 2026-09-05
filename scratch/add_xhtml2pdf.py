import re

file_path = 'app/pdf/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_html_to_pdf_bytes = '''def html_to_pdf_bytes(html: str, base_url: str | None = None) -> bytes:
    """
    Convert HTML string to PDF bytes.
    Priority:
      1. xhtml2pdf (Pure Python, very reliable for basic HTML)
      2. WeasyPrint (non-Windows, if available)
      3. wkhtmltopdf via pdfkit (Windows or fallback)
      4. CairoSVG (last resort)
    """
    # Try xhtml2pdf first (most reliable cross-platform without OS dependencies)
    try:
        from xhtml2pdf import pisa
        import io
        result = io.BytesIO()
        pisa_status = pisa.CreatePDF(io.StringIO(html), dest=result)
        if not pisa_status.err:
            return result.getvalue()
        else:
            current_app.logger.warning(f"xhtml2pdf failed with errors.")
    except Exception as e:
        current_app.logger.warning(f"xhtml2pdf crashed: {e}")

    # Try WeasyPrint on non-Windows
    if sys.platform != "win32" and WEASYPRINT_AVAILABLE:
        try:
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
        raise RuntimeError("PDF generation not available: no backend succeeded.")'''

old_html_to_pdf_bytes = r'def html_to_pdf_bytes\(html: str, base_url: str \| None = None\) -> bytes:.*?raise RuntimeError\("PDF generation not available: no backend succeeded\."\)'

text = re.sub(old_html_to_pdf_bytes, new_html_to_pdf_bytes, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
