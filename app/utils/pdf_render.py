import os, sys
import pdfkit

CANDIDATES = [
    os.getenv("WKHTMLTOPDF_EXE", ""),
    r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe",
    r"C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe",
]

def _find_wkhtml() -> str | None:
    for p in CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None

def html_to_pdf_bytes(html: str, base_url: str | None = None) -> bytes:
    # Try WeasyPrint on non-Windows; your error is Windows-specific.
    if sys.platform != "win32":
        try:
            from weasyprint import HTML  # lazy import
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
        # "page-size": "A4",
        # "margin-top": "12mm", "margin-right": "12mm",
        # "margin-bottom": "12mm", "margin-left": "12mm",
    }
    return pdfkit.from_string(html, False, configuration=cfg, options=options)
