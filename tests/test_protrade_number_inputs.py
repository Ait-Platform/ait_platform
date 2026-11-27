"""Database-free browser coverage for ProTrade numeric replacement entry."""
from pathlib import Path
from types import SimpleNamespace
import unittest

from flask import Blueprint, Flask, render_template
from jinja2 import ChoiceLoader, DictLoader
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'app/static/js/protrade_number_inputs.js'


class ProTradeNumberInputsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.chromium.launch()

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.page = self.browser.new_page()
        self.addCleanup(self.page.close)

    def price_list(self, trade):
        app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        app.jinja_loader = ChoiceLoader([DictLoader({
            'layout.html': '{% block content %}{% endblock %}',
            'partials/flash_messages.html': '',
        }), app.jinja_loader])
        app.jinja_env.globals['csrf_token'] = lambda: ''
        for endpoint in ('mechanic_dashboard', 'catalog_manage'):
            app.add_url_rule('/' + endpoint, 'mechanic_bp.' + endpoint, lambda: '')
        with app.test_request_context('/'):
            return render_template('program_mechanic/boq_price_list.html',
                shop=SimpleNamespace(id=1, business_name='Browser fixture', trade=trade),
                category=trade, categories=[], boq_items=[], edit_item=None,
                values={'material_rate': '12.34', 'labour_rate': '6.66'},
                units=['m', 'Each'], work_items=[], error=None)

    def test_price_list_keyboard_replacement_and_calculations(self):
        for trade in ('carpentry', 'plumbing', 'electrical', 'painting', 'building', 'general', 'boq'):
            with self.subTest(trade=trade):
                self.page.set_content(self.price_list(trade))
                self.page.add_script_tag(path=str(HELPER))
                quantity = self.page.locator('#boq-quantity')
                self.assertEqual(quantity.input_value(), '1')
                quantity.click()
                quantity.press_sequentially('2')
                self.assertEqual(quantity.input_value(), '2')
                quantity.press('Tab')
                self.page.keyboard.type('25.75')
                self.assertEqual(self.page.locator('#material-rate').input_value(), '25.75')
                self.page.keyboard.press('Tab')
                self.page.keyboard.type('4.25')
                self.assertEqual(self.page.locator('#labour-rate').input_value(), '4.25')
                self.assertEqual(self.page.locator('#total-rate').text_content(), '30.00')
                self.assertEqual(self.page.locator('#total-amount').text_content(), '60.00')
                material = self.page.locator('#material-rate')
                material.click()
                material.press_sequentially('0.50')
                self.assertEqual(material.input_value(), '0.50')
                material.press('ArrowUp')
                self.assertEqual(material.input_value(), '0.51')
                self.assertEqual(self.page.locator('#total-rate').text_content(), '4.76')

    def test_dynamic_controls_spinners_and_noneditable_fields(self):
        self.page.set_content('''<style>input {width:200px;height:40px;}</style>
            <input id="text" value="abc"><input type="number" id="readonly" readonly value="45">
            <fieldset disabled><input type="number" id="disabled" value="67"></fieldset>''')
        self.page.add_script_tag(path=str(HELPER))
        self.page.evaluate('''() => document.body.insertAdjacentHTML('beforeend',
            '<input id="dynamic" type="number" step="0.01" value="12.34">')''')
        dynamic = self.page.locator('#dynamic')
        dynamic.click()
        dynamic.press_sequentially('7.25')
        self.assertEqual(dynamic.input_value(), '7.25')
        # Exercise the actual native upper spinner button, not a simulated input event.
        dynamic.click(position={'x': 185, 'y': 12})
        self.assertEqual(dynamic.input_value(), '7.26')
        self.page.locator('#readonly').click()
        self.page.keyboard.type('9')
        self.assertEqual(self.page.locator('#readonly').input_value(), '45')
        self.assertEqual(self.page.locator('#disabled').input_value(), '67')
        self.page.locator('#text').focus()
        self.assertEqual(self.page.locator('#text').evaluate('(e) => e.selectionStart === e.selectionEnd'), True)

    def test_helper_is_scoped_to_protrade_in_shared_layout(self):
        app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        app.secret_key = 'browser-fixture'
        app.jinja_env.globals['csrf_token'] = lambda: ''
        bp = Blueprint('mechanic_bp', __name__)
        bp.add_url_rule('/protrade', 'fixture', lambda: '')
        app.register_blueprint(bp)
        app.add_url_rule('/other', 'other', lambda: '')
        for route, expected in (('/protrade', 1), ('/other', 0)):
            with app.test_request_context(route):
                rendered = render_template('layout.html', hide_navbar=True)
            self.assertEqual(rendered.count('js/protrade_number_inputs.js'), expected)


if __name__ == '__main__':
    unittest.main()
