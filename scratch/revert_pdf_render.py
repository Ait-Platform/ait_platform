import re

file_path = 'app/utils/pdf_render.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_func = '''def html_to_pdf_bytes(html: str, base_url: str | None = None, orientation: str = "Portrait") -> bytes:
    # Try WeasyPrint on non-Windows; your error is Windows-specific.
    if sys.platform != "win32":
        try:
            from weasyprint import HTML  # lazy import
            # Weasyprint orientation is set in CSS usually, but ignore for now
            return HTML(string=html, base_url=base_url).write_pdf()
        except Exception:
            pass

    exe = _find_wkhtml()
    if not exe:
        raise RuntimeError("wkhtmltopdf not found. Set WKHTMLTOPDF_EXE or install to the default path.")
    cfg = pdfkit.configuration(wkhtmltopdf=exe)
    options = {
        "encoding": "UTF-8",
        "enable-local-file-access": None,  # allow CSS/assets
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
