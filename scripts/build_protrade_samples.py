"""Build the fictional sample pack with the existing ProTrade PDF renderer."""
from io import BytesIO
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask, render_template
from pypdf import PdfReader, PdfWriter
from app.protrade_samples import SAMPLE_DATE, sample_documents
from app.utils.pdf_render import html_to_pdf_bytes


def build():
    app = Flask(__name__, template_folder=str(ROOT/'templates'))
    writer = PdfWriter()
    with app.app_context():
        for sample in sample_documents():
            html = render_template('program_mechanic/output_pdf.html',
                issuer=sample['issuer'], document=sample['document'], generated=SAMPLE_DATE+'T09:00:00')
            pdf = html_to_pdf_bytes(html)
            reader = PdfReader(BytesIO(pdf))
            if len(reader.pages) != 1:
                raise RuntimeError('Each sample must fit on one page')
            writer.add_page(reader.pages[0])
    target = ROOT/'app/static/pdf/protrade-samples.pdf'
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('wb') as stream:
        writer.write(stream)
    print(str(target))


if __name__ == '__main__':
    build()
