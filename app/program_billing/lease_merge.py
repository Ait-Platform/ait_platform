"""Stage G: pure bracket-placeholder DOCX merger, not an enabled generation service.

The caller must supply an independently approved master digest and explicit values.
No filesystem write, network request, approval or activation occurs in this module.
"""
import hashlib
import re
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from lxml import etree as ET

W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
TOKEN = re.compile(r'\[[^\[\]\r\n]+\]')


def merge_draft_docx(master, values, *, approved_sha256):
    digest = hashlib.sha256(master).hexdigest()
    if digest != approved_sha256:
        raise ValueError('The master does not match its approved immutable version.')
    if not isinstance(values, dict) or any(not isinstance(k, str) or not TOKEN.fullmatch(k)
            or not isinstance(v, str) or not v.strip() or TOKEN.search(v) for k, v in values.items()):
        raise ValueError('Supply explicit nonempty values for exact bracket placeholders.')
    parsed = {}; tokens = set()
    with ZipFile(BytesIO(master)) as source:
        entries = source.infolist()
        if len(entries) > 2000 or sum(e.file_size for e in entries) > 40 * 1024 * 1024:
            raise ValueError('Master exceeds supported document limits.')
        names = [e.filename for e in entries]
        if len(set(names)) != len(names) or 'word/document.xml' not in names or '[Content_Types].xml' not in names:
            raise ValueError('Invalid master DOCX package.')
        if any('vbaproject' in n.lower() or '..' in n.split('/') or n.startswith(('/', '\\')) for n in names):
            raise ValueError('Unsupported master package content.')
        for entry in entries:
            if entry.filename.startswith('word/') and entry.filename.endswith('.xml'):
                raw = source.read(entry)
                if b'<!DOCTYPE' in raw or b'<!ENTITY' in raw:
                    raise ValueError('Unsupported XML declarations.')
                root = ET.fromstring(raw, parser=ET.XMLParser(resolve_entities=False, no_network=True))
                if any('MERGEFIELD' in (e.text or '').upper() for e in root.iter('{'+W+'}instrText')):
                    raise ValueError('Word MERGEFIELD instructions require a separately reviewed mapping.')
                paragraphs = []
                for paragraph in root.iter('{'+W+'}p'):
                    nodes = list(paragraph.iter('{'+W+'}t'))
                    content = ''.join(node.text or '' for node in nodes)
                    found = list(TOKEN.finditer(content))
                    tokens.update(m[0] for m in found)
                    paragraphs.append((nodes, found))
                parsed[entry.filename] = (root, paragraphs)
        if tokens - values.keys():
            raise ValueError('Missing merge values: ' + ', '.join(sorted(tokens - values.keys())))
        if values.keys() - tokens:
            raise ValueError('Mapping contains placeholders absent from this master.')
        if not tokens: raise ValueError('Master has no supported bracket placeholders.')
        for root, paragraphs in parsed.values():
            for nodes, matches in paragraphs:
                offsets = []; position = 0
                for node in nodes:
                    text = node.text or ''; offsets.append((position, position + len(text))); position += len(text)
                for match in reversed(matches):
                    affected = [i for i, (start, end) in enumerate(offsets) if start < match.end() and end > match.start()]
                    first, last = affected[0], affected[-1]
                    before = (nodes[first].text or '')[:match.start() - offsets[first][0]]
                    after = (nodes[last].text or '')[match.end() - offsets[last][0]:]
                    nodes[first].text = before + values[match[0]] + (after if first == last else '')
                    nodes[first].set('{http://www.w3.org/XML/1998/namespace}space', 'preserve')
                    for index in affected[1:]: nodes[index].text = after if index == last else ''
        for root, _ in parsed.values():
            for node in list(root.iter('{'+W+'}t')):
                if '\n' not in (node.text or ''):continue
                lines=node.text.split('\n');node.text=lines[0];cursor=node
                for line in lines[1:]:
                    br=ET.Element('{'+W+'}br');cursor.addnext(br)
                    t=ET.Element('{'+W+'}t');t.text=line
                    t.set('{http://www.w3.org/XML/1998/namespace}space','preserve')
                    br.addnext(t);cursor=t
        target = BytesIO()
        with ZipFile(target, 'w', ZIP_DEFLATED) as output:
            for entry in entries:
                content = ET.tostring(parsed[entry.filename][0], encoding='utf-8', xml_declaration=True) if entry.filename in parsed else source.read(entry)
                output.writestr(entry, content)
    result = target.getvalue()
    return {'docx':result, 'master_sha256':digest, 'output_sha256':hashlib.sha256(result).hexdigest(),
            'merged_values':dict(values), 'status':'draft-not-approved'}
