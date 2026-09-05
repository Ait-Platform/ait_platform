import re

file_path = 'app/utils/pdf_render.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_func = '''def html_to_pdf_bytes(html: str, base_url: str | None = None, orientation: str = "Portrait") -> bytes:
    # 1. Try xhtml2pdf (Cross-platform, no OS dependencies)
    try:
        from xhtml2pdf import pisa
        import io
        result = io.BytesIO()
        pisa_status = pisa.CreatePDF(io.StringIO(html), dest=result)
        if not pisa_status.err:
            return result.getvalue()
    except Exception:
        pass

    # 2. Try WeasyPrint on non-Windows
    if sys.platform != "win32":
        try:
            from weasyprint import HTML  # lazy import
            return HTML(string=html, base_url=base_url).write_pdf()
        except Exception:
            pass

    # 3. Try wkhtmltopdf
    exe = _find_wkhtml()
    if not exe:
        raise RuntimeError("PDF generation failed: No PDF backend (xhtml2pdf, WeasyPrint, or wkhtmltopdf) was able to run in this environment.")
    cfg = pdfkit.configuration(wkhtmltopdf=exe)
    options = {
        "encoding": "UTF-8",
        "enable-local-file-access": None,
        "print-media-type": None,
        "quiet": None,
        "orientation": orientation,
        "margin-top": "10mm",
        "margin-right": "10mm",
        "margin-bottom": "10mm",
        "margin-left": "10mm",
    }
    return pdfkit.from_string(html, False, configuration=cfg, options=options)'''

old_func = r'def html_to_pdf_bytes.*?return pdfkit.from_string\(html, False, configuration=cfg, options=options\)'
text = re.sub(old_func, new_func, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
