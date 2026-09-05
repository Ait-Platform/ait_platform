import re

file_path = 'app/pdf/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Replace my giant html_to_pdf_bytes with the original one + fixed WEASYPRINT_AVAILABLE
new_func = '''def html_to_pdf_bytes(html: str, base_url: str | None = None) -> bytes:
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
        raise RuntimeError("PDF generation not available: no backend succeeded.")'''

old_func = r'def html_to_pdf_bytes.*?raise RuntimeError\("PDF generation not available: no backend succeeded\."\)'
text = re.sub(old_func, new_func, text, flags=re.DOTALL)

# Fix the import block at top
import_block = '''import sys, io
import pdfkit'''

text = re.sub(r'import sys, io\nimport pdfkit.*?WEASYPRINT_AVAILABLE = False', import_block, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
