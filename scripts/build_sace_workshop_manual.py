"""Reading take-away manual from the existing workshop slide source, unchanged."""
from pathlib import Path
from PIL import Image
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.utils import ImageReader

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'app/static/pdf/Reading_Workshop_Manual.pdf'


def build():
    width, height = landscape(A4)
    pdf = canvas.Canvas(str(OUTPUT), pagesize=(width, height), pageCompression=1, invariant=1)
    pdf.setTitle('Workshop Manual - I Learn to Read English Using the LITRE Method')
    pdf.setAuthor('AIT')
    for number in range(1, 32):
        slide = ROOT / f'app/static/sace_slides/{number}.png'
        with Image.open(slide) as source:
            encoded = BytesIO()
            source = source.convert('RGB')
            source.thumbnail((3000, 2200), Image.Resampling.LANCZOS)
            source.save(encoded, format='JPEG', quality=90, optimize=True)
            encoded.seek(0)
            image = ImageReader(encoded)
            iw, ih = image.getSize()
        pdf.setFont('Helvetica-Bold', 15)
        pdf.drawString(28, height - 28, 'Workshop Manual')
        pdf.setFont('Helvetica', 9)
        pdf.drawRightString(width - 28, height - 28, f'Workshop slide {number} of 31')
        scale = min((width - 56) / iw, (height - 85) / ih)
        dw, dh = iw * scale, ih * scale
        pdf.drawImage(image, (width - dw) / 2, 40 + ((height - 85) - dh) / 2, width=dw, height=dh)
        pdf.setFont('Helvetica', 8)
        pdf.drawString(28, 16, 'I Learn to Read English Using the LITRE Method - workshop content, slides 1-31.')
        pdf.showPage()
    pdf.save()


if __name__ == '__main__':
    build()
