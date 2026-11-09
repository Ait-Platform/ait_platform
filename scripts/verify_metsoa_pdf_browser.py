"""Deployment-only Chromium/PDF smoke check; no app startup or database access."""
import json
import os
from importlib.metadata import version
from playwright.sync_api import sync_playwright


def verify():
    if os.environ.get('PLAYWRIGHT_BROWSERS_PATH') != '0':
        raise RuntimeError('PLAYWRIGHT_BROWSERS_PATH must be 0 during build and runtime.')
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('<html><body><h1>METSOA PDF deployment check</h1></body></html>')
            pdf = page.pdf(format='A4')
            if not pdf.startswith(b'%PDF-') or len(pdf) < 1000:
                raise RuntimeError('Chromium did not produce a valid PDF.')
            return dict(playwright=version('playwright'), chromium=browser.version,
                        browser_path=os.environ['PLAYWRIGHT_BROWSERS_PATH'], pdf_bytes=len(pdf))
        finally:
            browser.close()


if __name__ == '__main__':
    print(json.dumps(verify(), sort_keys=True))
