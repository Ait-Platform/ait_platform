"""Presentation-only menu colours and Directory Register form placement."""
import unittest
from flask import Flask, render_template
from jinja2 import FileSystemLoader
from lxml import html
from test_pm_directory_register import ROOT, browser_html


class DirectoryLayoutTests(unittest.TestCase):
    def test_main_menu_colours_consistent_sizing_and_active_state(self):
        app=Flask('menu-layout');app.jinja_loader=FileSystemLoader(str(ROOT/'templates'))
        expected=[('Property & Owner','#eff6ff','#93c5fd'),('Lease','#f0fdf4','#86efac'),('Property Portfolio','#fefce8','#fcd34d'),('Directory Register','#f5f3ff','#c4b5fd')]
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
            self.assertIn('flex:1 1 170px',link.get('style'))
            self.assertIn('border',link.get('class').split())
        self.assertEqual(links[-1].get('aria-current'),'page')
        self.assertIn('box-shadow',links[-1].get('style'))

    def test_directory_page_retains_hub_without_duplicate_menu(self):
        tree=html.fromstring(browser_html())
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]'))
        self.assertEqual(tree.xpath('//a[contains(text(),"Back to Property Management")]/text()'),['Back to Property Management'])
        for identifier in ['directory-add','directory-view','directory-close','directory-form','directory-table']:
            self.assertIsNotNone(tree.get_element_by_id(identifier))
        self.assertIsNotNone(tree.get_element_by_id('directory-workspace').get('hidden'))

    def test_cancel_save_share_right_aligned_row(self):
        tree=html.fromstring(browser_html());row=tree.get_element_by_id('directory-form-actions')
        self.assertEqual(row.xpath('./button/text()'),['Cancel','Save'])
        self.assertTrue({'flex','items-center','justify-end','whitespace-nowrap'}.issubset(row.get('class').split()))
        self.assertEqual(row.xpath('./button/@type'),['button','submit'])
        self.assertEqual(row.getparent().get('id'),'directory-form')


if __name__=='__main__':unittest.main()
