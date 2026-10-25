"""Private, review-only Lease Kit PDFs. No persistence or issuance side effects."""
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
import sys

CORE = Path(__file__).with_name('private_templates') / 'AIT_Residential_Core_Final_Review.pdf'
CORE_BUCKET = 'ait-lease-controlled-private'
CORE_KEY = 'AIT_Residential_Core_Final_Review.pdf'
CORE_SHA256 = '3442a3a250ddd7fde1fabd6040409b338d0bbd302b6014a84073c3c0a196441c'
REVIEW = 'DRAFT \u2014 FOR REVIEW ONLY \u2014 NOT FOR SIGNATURE OR ISSUANCE'


def core_bytes():
    from . import lease_r2
    try:
        data = lease_r2.read_file_from_r2(CORE_KEY, bucket=CORE_BUCKET)
    except Exception:
        # Storage errors may contain signed request details; never expose them.
        raise ValueError('Private Residential Core retrieval is unavailable.') from None
    if not isinstance(data, bytes) or not data.startswith(b'%PDF-'):
        raise ValueError('Residential Core is not a PDF')
    if sha256(data).hexdigest() != CORE_SHA256:
        raise ValueError('Residential Core integrity check failed')
    from pypdf import PdfReader
    try:
        reader = PdfReader(BytesIO(data), strict=True)
        if reader.is_encrypted or len(reader.pages) != 9:
            raise ValueError()
        for page in reader.pages:
            page.get_contents()
    except Exception:
        raise ValueError('Residential Core must be a valid, unencrypted nine-page PDF') from None
    return data


def distinct_parties(parties):
    """Identity-based display grouping only; never merge by name or mutate snapshots."""
    result = []
    def normalized(value):
        return re.sub(r'[^a-z0-9]', '', str(value or '').casefold())
    for party in parties:
        if not isinstance(party, dict):
            continue
        match = None
        for saved in result:
            a, b = normalized(party.get('identity_number')), normalized(saved.get('identity_number'))
            if a and b and a != b:
                continue
            same_register = any(party.get(key) is not None and str(party[key]) == str(saved.get(key))
                                for key in ('tenant_id', 'owner_id'))
            def category(row):
                value = row.get('party_type') or row.get('type')
                if value in ('individual', 'natural_person'):return 'natural'
                if value in ('company', 'juristic_person', 'npc', 'trust'):return 'juristic'
                return value
            kind_a, kind_b = category(party), category(saved)
            if kind_a and kind_b and kind_a != kind_b:
                continue
            if same_register or (a and a == b):
                match = saved
                break
        if match is None:
            result.append(deepcopy(party))
        else:
            for key, value in party.items():
                if not match.get(key) and value:
                    match[key] = deepcopy(value)
    return result


def signature_parties(items):
    from .lease_certificate import selected_tenants
    lessors = distinct_parties(items.get('_contracting_lessors', {}).get('identities', []))
    lessees = distinct_parties(selected_tenants(items))
    representative = items.get('_parties', {}).get('lessor', {})
    for party in lessors:
        owner_match = representative.get('owner_id') is not None and str(party.get('owner_id')) == str(representative['owner_id'])
        identity_match = bool(party.get('identity_number') and str(party['identity_number']) == str(representative.get('signatory_identity_number')))
        if owner_match or identity_match:
            for key in ('signatory_name', 'signatory_identity_number', 'signatory_capacity'):
                if not party.get(key) and representative.get(key):
                    party[key] = representative[key]
    return lessors, lessees


def signature_pdf(context):
    """The same saved signing particulars and HTML fragment used by View Lease."""
    return html_pdf('signature', context)


def stable_review_pdf(payload):
    """Drop renderer timestamps so unchanged review PDFs retain an exact email hash."""
    from pypdf import PdfReader, PdfWriter
    reader = PdfReader(BytesIO(payload))
    writer = PdfWriter()
    writer.append(reader)
    if reader.metadata:
        writer.add_metadata({key: str(value) for key, value in reader.metadata.items()
                             if key not in ('/CreationDate', '/ModDate')})
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def html_pdf(component, context):
    from flask import current_app
    from jinja2 import ChoiceLoader, FileSystemLoader
    env = current_app.jinja_env.overlay(loader=ChoiceLoader([
        current_app.jinja_env.loader,
        FileSystemLoader(str(Path(current_app.root_path).parent / 'templates'))]))
    template = env.get_template('program_billing/lease_kit_pdf.html')
    html = template.render(**dict(context, pdf_component=component, review_notice=REVIEW))
    names = ('lease_kit.css', 'lease_certificate.css') if component == 'certificate' else ('lease_kit.css',)
    css = '\n'.join((Path(current_app.root_path) / 'static/css' / name).read_text(encoding='utf-8') for name in names)
    if sys.platform == 'win32':
        from app.utils.pdf_render import html_to_pdf_bytes
        # Reuse the configured Windows converter; embedded CSS needs no resource fetches.
        fallback_css = '@media print { #approved-certificate .certificate-page footer { display:block !important; position:static !important; margin-top:6mm; text-align:right; } }'
        html = html.replace('<style>', '<style>'+css+'\n'+fallback_css+'\n', 1)
        return stable_review_pdf(html_to_pdf_bytes(html))
    from weasyprint import HTML, CSS
    styles = [CSS(string=css)]
    # Reject network/local resource requests from rendered content. All content is escaped text.
    def no_resources(url):
        raise ValueError('External PDF resources are disabled')
    return stable_review_pdf(HTML(string=html, url_fetcher=no_resources).write_pdf(stylesheets=styles))


def build_pdf(component, context):
    draft=context.get('lease_draft');filed=bool(draft and draft['items'].get('_sc_pdf_current'));sc_pdf=None
    if filed and component in ('special_conditions','lease'):
        from .lease_sc_pdf import retrieve
        sc_pdf,_=retrieve(context['property'],draft)
    if component in ('lease','special_conditions') and not filed and context['states']['special_conditions'].get('legacy_conditions'):
        raise ValueError('Legacy Supplementary Conditions require review before assembly')
    from pypdf import PdfReader, PdfWriter
    if component == 'core':
        return core_bytes()
    if component=='special_conditions' and filed:return sc_pdf
    if component != 'lease':
        return html_pdf(component, context)
    writer = PdfWriter()
    for key in ('cover', 'certificate', 'core', 'special_conditions', 'signature'):
        payload = core_bytes() if key == 'core' else sc_pdf if key=='special_conditions' and filed else html_pdf(key, context)
        writer.append(PdfReader(BytesIO(payload)))
        if key=='special_conditions' and not filed and context.get('lease_draft') and context['lease_draft']['items'].get('_map_annexures'):
            from .lease_map_annexure import compiled
            for annexure in compiled(context):writer.append(PdfReader(BytesIO(annexure)))
    writer.add_metadata({'/Title': 'AIT Lease Kit - REVIEW ONLY', '/Subject': REVIEW})
    output = BytesIO()
    writer.write(output)
    return output.getvalue()
