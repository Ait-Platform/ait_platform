"""Render checks and actual viewer JavaScript navigation, without DB or network."""
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest

from jinja2 import DictLoader, Environment


ROOT = Path(__file__).resolve().parents[1]
VIEWER_TITLES = ('Application Form 1', 'Application Form 2', 'Facilitator Manual', 'Workshop Manual')


def render(title):
    source = (ROOT / 'templates/program_sace/endorsement_material.html').read_text(encoding='utf-8')
    env = Environment(loader=DictLoader({
        'viewer.html': source,
        'layout.html': '{% block content %}{% endblock %}',
    }))
    env.globals.update(url_for=lambda *args: '/sace/reading',
                       csrf_token=lambda: 'fixture-csrf',
                       get_flashed_messages=lambda **kwargs: [])
    return env.get_template('viewer.html').render(
        doc_title=title, doc_url='/fixture.pdf', viewed_url='/fixture/viewed')


class ReadingApplicationViewer(unittest.TestCase):
    def test_forms_and_manuals_render_shared_presentation(self):
        for title in VIEWER_TITLES:
            with self.subTest(title=title):
                html = render(title)
                self.assertIn(title + ' may take a few seconds to load.', html)
                self.assertRegex(html, r'class="border bg-indigo-700 text-white[^" ]*.*?href="/sace/reading"')
                for element, colour in (('previous', 'blue'), ('next', 'emerald')):
                    tag = re.search(r'<button id="' + element + r'"[^>]*>', html).group()
                    self.assertIn(' hidden', tag)
                    self.assertIn('bg-' + colour + '-50', tag)
                self.assertIn('id="page" class="border rounded p-2 bg-slate-50', html)
                self.assertIn('id="document" class="' + ('max-w-none' if 'Manual' in title else 'max-w-full') + ' mx-auto border border-blue-400"', html)
                if 'Manual' in title:
                    self.assertIn('class="overflow-x-auto"', html)

    def test_other_material_keeps_existing_controls(self):
        html = render('Other controlled material')
        self.assertNotIn('may take a few seconds to load.', html)
        self.assertIn('<button id="previous" class="border rounded p-2">', html)
        self.assertIn('<button id="next" class="border rounded p-2">', html)
        self.assertNotIn('.hidden=', html)
        self.assertIn('bg-blue-50 text-blue-900 border-blue-200', html)

    def test_timetable_has_no_pagination_and_retains_viewer_styling(self):
        html = render('Reading Timetable (T/T)')
        self.assertIn('Reading Timetable (T/T) may take a few seconds to load.', html)
        for element in ('previous', 'next', 'page'):
            self.assertNotIn('id="' + element + '"', html)
            self.assertNotIn("getElementById('" + element + "')", html)
        self.assertNotIn('Page 1 of 1', html)
        self.assertIn('bg-indigo-700 text-white', html)
        self.assertIn('href="/sace/reading"', html)
        self.assertIn('id="document" class="max-w-full mx-auto border border-blue-400"', html)

    def test_actual_javascript_first_middle_last_and_reverse_navigation(self):
        node = shutil.which('node')
        if node is None:
            spec = importlib.util.find_spec('playwright')
            if spec and spec.origin:
                candidate = Path(spec.origin).parent / 'driver' / ('node.exe' if __import__('os').name == 'nt' else 'node')
                if candidate.is_file():
                    node = str(candidate)
        self.assertIsNotNone(node, 'Node or the existing Playwright bundled Node is required')
        for title in VIEWER_TITLES + ('Reading Timetable (T/T)',):
            for total in ((31, 1) if title in ('Facilitator Manual', 'Workshop Manual') else (1,) if title == 'Reading Timetable (T/T)' else (7, 1)):
                with self.subTest(title=title, total=total):
                    script = re.search(r'<script>\s*(.*?)\s*</script>', render(title), re.S).group(1)
                    harness = r'''
const assert = require('node:assert/strict');
const total = TOTAL;
const timetable = TIMETABLE;
const expectedScale = SCALE;
const elements = Object.fromEntries((timetable ? ['status','document'] : ['status','previous','next','page','document']).map(id =>
    [id, {hidden: true, textContent: '', getContext: () => ({})}]));
global.document = {getElementById: id => elements[id]};
let delivered = 0;
global.fetch = async (url, options) => {
    assert.equal(url, '/fixture/viewed');
    assert.equal(options.method, 'POST');
    assert.equal(options.headers['X-CSRFToken'], 'fixture-csrf');
    delivered++; return {ok: true};
};
global.pdfjsLib = {GlobalWorkerOptions: {}, getDocument: url => {
    assert.equal(url, '/fixture.pdf');
    return {promise: Promise.resolve({numPages: total, getPage: async () => ({
        getViewport: options => {assert.equal(options.scale, expectedScale); return {width: 600, height: 800};},
        render: () => ({promise: Promise.resolve()})
    })})};
}};
const tick = () => new Promise(resolve => setImmediate(resolve));
function check(page) {
    if (timetable) {assert.equal(elements.page, undefined); return;}
    assert.equal(elements.previous.hidden, page === 1);
    assert.equal(elements.next.hidden, page === total);
    assert.equal(elements.page.textContent, `Page ${page} of ${total}`);
}
(async () => {
    await eval(SCRIPT);
    check(1);
    for (let page = 2; page <= total; page++) {
        elements.next.onclick(); await tick(); check(page);
    }
    assert.equal(delivered, 1);
    for (let page = total - 1; page >= 1; page--) {
        elements.previous.onclick(); await tick(); check(page);
    }
    assert.equal(delivered, 1);
})().catch(error => { console.error(error); process.exitCode = 1; });
'''.replace('TOTAL', str(total)).replace('TIMETABLE', 'true' if title == 'Reading Timetable (T/T)' else 'false').replace('SCALE', '2' if 'Manual' in title else '1.3').replace('SCRIPT', json.dumps(script))
                    result = subprocess.run([node, '-'], input=harness, text=True,
                                            capture_output=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
