"""Presentation-only menu colours and Directory Register form placement."""
import unittest
from flask import Flask, render_template
from jinja2 import FileSystemLoader
from lxml import html
from test_pm_directory_register import ROOT, browser_html


class DirectoryLayoutTests(unittest.TestCase):
    def test_main_menu_colours_consistent_sizing_and_active_state(self):
        app=Flask('menu-layout');app.jinja_loader=FileSystemLoader(str(ROOT/'templates'))
        expected=[('Property & Owner','#eff6ff','#93c5fd'),('Lease','#f0fdf4','#86efac'),('Property Portfolio','#fefce8','#fcd34d'),('Folders','#f5f3ff','#c4b5fd')]
        with app.test_request_context():
            rendered=render_template('program_billing/menu.html',pm_menu_active='directories',url_for=lambda endpoint,**kw:'/'+endpoint)
        links=html.fromstring(rendered).xpath('//a')
        self.assertEqual(len(links),4)
        for link,(label,background,border) in zip(links,expected):
            self.assertEqual(link.text,label)
            self.assertIn('background-color:'+background,link.get('style'))
            self.assertIn('border-color:'+border,link.get('style'))
            self.assertIn('color:#0f172a',link.get('style'))
            self.assertIn('min-height:46px',link.get('style'))
            self.assertIn('flex:1 1 0',link.get('style'))
            self.assertIn('border',link.get('class').split())
        self.assertEqual(links[-1].get('aria-current'),'page')
        self.assertIn('box-shadow',links[-1].get('style'))

    def test_directory_page_retains_hub_without_duplicate_menu(self):
        tree=html.fromstring(browser_html())
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]'))
        self.assertEqual(tree.xpath('//a[contains(text(),"Back to Property Management")]/text()'),['Back to Property Management'])
        for identifier in ['directory-select','directory-view','directory-close','directory-change','directory-reauthorise']:
            self.assertIsNotNone(tree.get_element_by_id(identifier))
        self.assertIsNotNone(tree.get_element_by_id('directory-workspace').get('hidden'))

    def test_folders_replaces_per_report_form_with_one_main_folder(self):
        tree=html.fromstring(browser_html())
        self.assertEqual(tree.xpath('//h1/text()'),['Folders'])
        self.assertFalse(tree.xpath('//*[@id="directory-report" or @id="directory-form"]'))
        self.assertEqual(tree.xpath('//*[@id="directory-select"]/text()'),['Select Folder'])
        self.assertEqual(len(tree.xpath('//section[@id="directory-workspace"]//ul/ li/ul/li')),7)

    def test_four_menu_buttons_fit_one_desktop_row(self):
        from playwright.sync_api import sync_playwright
        app=Flask('desktop-menu');app.jinja_loader=FileSystemLoader(str(ROOT/'templates'))
        with app.test_request_context():
            markup=render_template('program_billing/menu.html',pm_menu_active='directories',url_for=lambda endpoint,**kw:'/'+endpoint)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport=dict(width=1024,height=768));page.set_content('<div style="width:700px">'+markup+'</div>')
                boxes=page.locator('nav a').evaluate_all('(links)=>links.map(a=>({top:a.getBoundingClientRect().top,right:a.getBoundingClientRect().right}))')
                self.assertEqual(len({b['top'] for b in boxes}),1)
                self.assertLessEqual(boxes[-1]['right'],708)
            finally:browser.close()


if __name__=='__main__':unittest.main()
