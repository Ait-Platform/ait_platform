"""Inspect exported synthetic UIP pages in local Chromium; no live database access."""
import json
from pathlib import Path
import sys
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]


def main():
    folder = (ROOT / "scratch/uip_ui_preview").resolve()
    assert folder.is_relative_to((ROOT / "scratch").resolve())
    pages = sorted(folder.glob("*.html"))
    assert len(pages) == 11
    findings = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
        for file in pages:
            page.goto("about:blank")
            page.set_content(file.read_text(encoding="utf-8"), wait_until="networkidle", timeout=30000)
            page.screenshot(path=str(folder / (file.stem + "-desktop.png")), full_page=True)
            dimensions = page.evaluate("""() => ({width:innerWidth, body:document.documentElement.scrollWidth,
                sidebar:document.querySelector('.ui-sidebar').getBoundingClientRect().width,
                navCount:document.querySelectorAll('[aria-label="UIP Command Centre"]').length,
                headings:document.querySelectorAll('h1').length,
                titleSize:getComputedStyle(document.querySelector('h1')).fontSize,
                columns:new Set(Array.from(document.querySelectorAll('.ui-navigation a')).map(a=>Math.round(a.getBoundingClientRect().left))).size})""")
            assert dimensions["body"] <= 1440, (file.name, dimensions)
            assert dimensions["sidebar"] == 238 and dimensions["navCount"] == 1 and dimensions["columns"] == 1, (file.name, dimensions)
            assert dimensions["headings"] == 1
            assert float(dimensions["titleSize"].removesuffix("px")) >= 26
            findings.append(dict(page=file.stem, desktop=dimensions))
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_function("!document.querySelector('.ui-mobile-menu').open")
            assert page.evaluate('document.documentElement.scrollWidth') <= 390, file.name
            page.screenshot(path=str(folder / (file.stem + "-mobile.png")), full_page=True)
            page.set_viewport_size({"width": 1440, "height": 1000})
            print("Desktop/mobile checked:", file.stem, flush=True)
        # Browser interaction: expanding/collapsing menu and intake search/reordering.
        page.goto("about:blank")
        page.set_content((folder / "log-interaction.html").read_text(encoding="utf-8"), wait_until="networkidle")
        page.locator('[data-filter-select="member_id"]').fill('Asha')
        assert page.locator('#member_id option[value]:not([value=""])').evaluate_all('(xs)=>xs.filter(x=>!x.hidden).length') == 1
        assert page.locator('#property_id optgroup[label="Linked properties"] option').count() == 1
        page.locator('#general-enquiry').click()
        assert page.locator('#member_id').input_value() == ''
        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_function("!document.querySelector('.ui-mobile-menu').open")
        page.locator('.ui-mobile-menu summary').click()
        assert page.locator('.ui-mobile-menu').evaluate('(e)=>e.open')
        browser.close()
    (folder / "inspection.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
    print("11 desktop and 11 mobile screenshots; one vertical navigation, no document overflow; mobile menu and intake interaction checks passed.")


if __name__ == "__main__":
    main()
