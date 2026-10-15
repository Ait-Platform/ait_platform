"""Portfolio PDF pagination; reuse the platform HTML renderer and installed PDF tools."""
from io import BytesIO
import uuid
from flask import render_template, request
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm


def build_portfolio_pdf(context):
    from app.utils.pdf_render import html_to_pdf_bytes
    groups=context['properties'] or [None]
    pages=[]
    report_id=uuid.uuid4().hex[:12].upper()
    for property_record in groups:
        subset=dict(context,properties=[property_record] if property_record else [])
        html=render_template("program_billing/portfolio_document.html",**subset)
        source=html_to_pdf_bytes(html,base_url=request.host_url,orientation="Portrait")
        reader=PdfReader(BytesIO(source))
        if not reader.pages:raise ValueError("Report renderer returned no pages")
        label=f"{property_record['name']} | Property ID {property_record['id']}" if property_record else "No active properties"
        for page in reader.pages:pages.append((page,label))
    writer=PdfWriter()
    total=len(pages)
    for index,(page,label) in enumerate(pages,1):
        width=float(page.mediabox.width);height=float(page.mediabox.height)
        overlay=BytesIO();stamp=canvas.Canvas(overlay,pagesize=(width,height))
        stamp.setFont('Helvetica-Bold',8)
        stamp.drawString(16*mm,height-5*mm,'AIT Platform | Property Portfolio')
        stamp.setFont('Helvetica',7)
        # Fit the complete property label into the available header width.
        size=7
        while stamp.stringWidth(label,'Helvetica',size)>width-32*mm and size>4:
            size-=0.25
        stamp.setFont('Helvetica',size);stamp.drawString(16*mm,height-8*mm,label)
        stamp.setFont('Helvetica',8)
        stamp.drawString(16*mm,7*mm,'AIT Portfolio '+report_id)
        stamp.drawRightString(width-16*mm,7*mm,f'Page {index} of {total}')
        stamp.save();overlay.seek(0)
        page.merge_page(PdfReader(overlay).pages[0]);writer.add_page(page)
    writer.add_metadata({'/Title':'AIT Platform - Property Portfolio','/Author':'AIT Platform','/Subject':'Authorized Property Portfolio '+report_id})
    result=BytesIO();writer.write(result);return result.getvalue()
