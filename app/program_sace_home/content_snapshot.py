"""Private HOME content bundles. No application factory or participant routes."""
import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace
from jinja2 import DictLoader, Environment, StrictUndefined, select_autoescape

SCHEMA = 'home-auditor-content-v1'
SUBJECT = 'sace_home_endorsement'
STAGES = {'practical': range(1, 11), 'application': range(11, 21),
          'theory': range(21, 31), 'review': range(21, 31)}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


class InertHTML(HTMLParser):
    """Allow educational markup only; never retain forms, links or executable code."""
    allowed = {'div', 'p', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol',
               'li', 'strong', 'b', 'em', 'i', 'table', 'thead', 'tbody', 'tr', 'th',
               'td', 'br', 'hr', 'label', 'img'}
    suppressed = {'script', 'style', 'button', 'iframe', 'object', 'svg'}

    def __init__(self, assets):
        super().__init__(convert_charrefs=True)
        self.assets, self.parts, self.depth = assets, [], 0

    def handle_starttag(self, tag, attrs):
        if tag in self.suppressed:
            self.depth += 1
        if self.depth or tag not in self.allowed:
            return
        attrs = dict(attrs)
        safe = ''
        # No hidden/modals: explanations and summaries are expanded for examination.
        classes = [x for x in attrs.get('class', '').split()
                   if re.fullmatch(r'[a-zA-Z0-9_:/.-]+', x) and x not in
                   {'hidden', 'fixed', 'inset-0', 'overflow-hidden', 'max-h-90vh'}]
        if classes:
            safe += ' class="' + escape(' '.join(classes), quote=True) + '"'
        for key in ('colspan', 'rowspan'):
            if attrs.get(key, '').isdigit():
                safe += ' ' + key + '="' + attrs[key] + '"'
        if tag == 'img':
            src = attrs.get('src', '')
            if not src.startswith('home-asset:') or src[11:] not in self.assets:
                return
            safe += ' src="' + escape(src, quote=True) + '" alt="' + escape(attrs.get('alt', ''), quote=True) + '"'
        self.parts.append('<' + tag + safe + '>')

    def handle_endtag(self, tag):
        if tag in self.suppressed:
            self.depth = max(0, self.depth - 1)
            return
        if not self.depth and tag in self.allowed and tag not in {'img', 'br', 'hr'}:
            self.parts.append('</' + tag + '>')

    def handle_data(self, data):
        if not self.depth:
            self.parts.append(escape(data))


def inert(html, assets):
    parser = InertHTML(assets)
    parser.feed(html)
    return ''.join(parser.parts)


def build(inventory, root, approved_by, approval_reference):
    if inventory['gaps'] or not approved_by.strip() or not approval_reference.strip():
        raise ValueError('Complete HOME source and explicit source approval are required.')
    root = Path(root).resolve()
    source = inventory['source']
    if digest(source) != inventory['source_sha256']:
        raise ValueError('Source digest mismatch.')
    files = {p: v['text'] for p, v in source['files'].items()}
    hashes = {p: v['sha256'] for p, v in source['files'].items()}
    for path in ('templates/subject_home/certificate.html', 'templates/shared/ait_certificate_layout.html',
                 'templates/subject_home/final_exam.html'):
        raw = (root / path).read_bytes()
        files[path] = raw.decode('utf-8-sig')
        hashes[path] = hashlib.sha256(raw).hexdigest()
    assets = {}
    for path, expected in sorted(source['assets'].items()):
        target = (root / path).resolve()
        if not target.is_relative_to(root / 'app/static/images'):
            raise ValueError('Invalid HOME image path.')
        raw = target.read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError('Source asset changed during snapshot.')
        key = hashlib.sha256(raw).hexdigest()
        suffix = target.suffix.lower()
        if suffix not in {'.png', '.jpg', '.jpeg', '.gif', '.webp'}:
            raise ValueError('Unsupported inert image type.')
        assets[key] = {'sha256': key, 'data': base64.b64encode(raw).decode('ascii'),
                       'mime': 'image/png' if suffix == '.png' else 'image/jpeg' if suffix in {'.jpg', '.jpeg'} else 'image/' + suffix[1:]}
    def source_url(endpoint, **values):
        if endpoint != 'static':
            return ''  # No participant endpoint resolution or execution.
        path = 'app/static/' + values['filename']
        key = source['assets'].get(path)
        if key is None:
            raise ValueError('Unmanifested educational asset.')
        return 'home-asset:' + key
    templates = {p.removeprefix('templates/'): t for p, t in files.items() if p.startswith('templates/')}
    templates['layout.html'] = '{% block content %}{% endblock %}'
    env = Environment(loader=DictLoader(templates), undefined=StrictUndefined,
                      autoescape=select_autoescape(['html']))
    env.globals.update(url_for=source_url, csrf_token=lambda: '')
    from .manual_sources import FALLBACK_IMAGES
    by_number = {c['chapter_number']: c for c in source['chapters']}
    if set(by_number) != set(range(1, 31)):
        raise ValueError('All 30 HOME chapter numbers are required.')
    chapters = []
    for number in range(1, 31):
        chapter = dict(by_number[number])
        qs = []
        for q in sorted(source['questions'], key=lambda q: q['id']):
            if q['chapter_id'] == chapter['id']:
                question = dict(q)
                question['options'] = sorted([dict(o) for o in source['options'] if o['question_id'] == q['id']],
                                             key=lambda o: (o['sort_order'], o['id']))
                qs.append(question)
        if number >= 11 and len(qs) < (5 if number >= 21 else 1):
            raise ValueError('Incomplete HOME question bank.')
        base = (number - 1) % 10 + 1
        image = by_number[base].get('image_filename') or FALLBACK_IMAGES[base]
        chapter.update(questions=qs, image_asset=source['assets']['app/static/images/' + image])
        view_qs = [SimpleNamespace(**q, shuffled_options=[SimpleNamespace(**o) for o in q['options']]) for q in qs]
        chapter['html'] = ''
        if number <= 10 or number >= 21:
            name = f'subject_home/chapter{number}_' + ('practical.html' if number <= 10 else 'theory.html')
            rendered = env.get_template(name).render(chapter=SimpleNamespace(**chapter), hero_image=image,
                questions=view_qs, is_teacher_scoring=True, submission=SimpleNamespace(id=0))
            if number >= 21:
                # The wrapper renders the question bank once, with its exact identities
                # and answer evidence. Preserve the objective and expanded theory.
                rendered = re.sub(r'<form\b[^>]*>.*?</form>', '', rendered, flags=re.S | re.I)
            chapter['html'] = inert(rendered, assets)
        chapters.append(chapter)
    assessment = [{'chapter_number': c['chapter_number'], 'chapter_id': c['id'], 'questions': c['questions'][:5]}
                  for c in chapters if c['chapter_number'] >= 21]
    sample = dict(id='SPECIMEN-NONISSUED', created_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
                  overall_score=80, observation_score=80, position_score=80, comparison_score=80,
                  estimation_score=80, measurement_score=80, pattern_score=80, spatial_score=80,
                  logic_score=80, mathematics_score=80, critical_thinking_score=80)
    specimens = {}
    for passed in (True, False):
        scores = dict(sample)
        for key in scores:
            if key.endswith('_score'):
                scores[key] = 80 if passed else 60
        rendered = env.get_template('subject_home/certificate.html').render(
            current_user=SimpleNamespace(name='Sample HOME Learner', first_name='Sample', last_name='Learner'),
            assessment=SimpleNamespace(**scores, passed=passed), logo_path='', seal_path='')
        specimens['passed' if passed else 'failed'] = inert(rendered, assets)
    payload = {'schema': SCHEMA, 'subject': SUBJECT, 'source_sha256': inventory['source_sha256'],
        'source_hashes': hashes, 'approval': {'approved_by': approved_by, 'reference': approval_reference},
        'chapters': chapters, 'assets': assets, 'assessment': assessment,
        'assessment_policy': 'First five questions by question ID per chapter number 21-30; fixed 50-question specimen. '
            'Exact answer match; multi-select compares the complete set. Each category is correct/5 x 100, rounded; '
            'overall is the rounded mean of ten category percentages; participant threshold is 70%. No Auditor grading.',
        'certificate_specimens': specimens}
    validate(payload)
    return {'version': digest(payload), 'manifest': payload}


def validate(manifest):
    if manifest.get('schema') != SCHEMA or manifest.get('subject') != SUBJECT:
        raise ValueError('Not a HOME Auditor content manifest.')
    chapters = manifest['chapters']
    if [c['chapter_number'] for c in chapters] != list(range(1, 31)) or len({c['id'] for c in chapters}) != 30:
        raise ValueError('HOME chapters must be unique and complete.')
    if not manifest['approval'].get('approved_by') or not manifest['approval'].get('reference'):
        raise ValueError('Missing HOME source approval.')
    for key, asset in manifest['assets'].items():
        if hashlib.sha256(base64.b64decode(asset['data'], validate=True)).hexdigest() != key or asset['sha256'] != key:
            raise ValueError('HOME asset digest mismatch.')
        if asset['mime'] not in {'image/png', 'image/jpeg', 'image/gif', 'image/webp'}:
            raise ValueError('Unsafe asset type.')
    ids, option_ids = set(), set()
    for c in chapters:
        if c['image_asset'] not in manifest['assets'] or inert(c['html'], manifest['assets']) != c['html']:
            raise ValueError('Unsafe educational presentation.')
        if c['chapter_number'] >= 11 and not c['questions']:
            raise ValueError('Missing questions.')
        if c['questions'] != sorted(c['questions'], key=lambda q: q['id']):
            raise ValueError('Unordered question identities.')
        for q in c['questions']:
            if q['id'] in ids or q['chapter_id'] != c['id'] or q['question_type'] not in {'select', 'single_select', 'multi_select'}:
                raise ValueError('Invalid HOME question identity/type.')
            ids.add(q['id'])
            if not q['options'] or q['options'] != sorted(q['options'], key=lambda o: (o['sort_order'], o['id'])):
                raise ValueError('Invalid option ordering.')
            if any(o['question_id'] != q['id'] for o in q['options']):
                raise ValueError('Invalid option identity.')
            for option in q['options']:
                if option['id'] in option_ids:
                    raise ValueError('Duplicate HOME option identity.')
                option_ids.add(option['id'])
            answers = [x.strip() for x in q['correct_answer'].split(',')] if q['question_type'] == 'multi_select' else [q['correct_answer']]
            if not q['question'] or not set(answers).issubset({o['option_text'] for o in q['options']}):
                raise ValueError('Invalid HOME answer key.')
    expected = [{'chapter_number': c['chapter_number'], 'chapter_id': c['id'], 'questions': c['questions'][:5]}
                for c in chapters if c['chapter_number'] >= 21]
    if manifest['assessment'] != expected or any(len(c['questions']) != 5 for c in expected):
        raise ValueError('Invalid fixed assessment specimen.')
    for html in manifest['certificate_specimens'].values():
        if inert(html, manifest['assets']) != html or 'Sample HOME Learner' not in html or 'SPECIMEN-NONISSUED' not in html:
            raise ValueError('Unsafe certificate specimen.')


def write_bundle(bundle, directory):
    directory = Path(directory).resolve()
    if 'static' in directory.parts:
        raise ValueError('Examination bundles must be private, outside static directories.')
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (bundle['version'] + '.json')
    encoded = canonical(bundle).encode('utf-8')
    if target.exists():
        if target.read_bytes() != encoded:
            raise ValueError('Refusing to overwrite immutable content.')
    else:
        with target.open('xb') as output:
            output.write(encoded)
    # Selecting a version affects only previously unbound Phase 2B examinations.
    pending = directory / 'CURRENT.tmp'
    pending.write_text(bundle['version'], encoding='ascii')
    pending.replace(directory / 'CURRENT')
    return target
