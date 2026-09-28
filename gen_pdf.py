from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import datetime

def create_founding_pdf(filename):
    c = canvas.Canvas(filename, pagesize=letter)
    width, height = letter
    
    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(width / 2.0, height - 80, "URBAN IMPROVEMENT PRECINCT")
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(width / 2.0, height - 105, "OFFICIAL FOUNDING DECLARATION & MANDATE")
    
    # Line
    c.line(50, height - 120, width - 50, height - 120)
    
    # Content
    c.setFont("Helvetica", 12)
    text = [
        "Date of Meeting: September 15, 2026",
        "Venue: MG Primary School Hall",
        "",
        "This document serves as the official recording of the Inaugural Public Meeting.",
        "It is hereby declared that:",
        "",
        "1. The boundaries of the Precinct were formally agreed upon.",
        "2. The official Constitution of the UIP was adopted by a majority vote of ratepayers.",
        "3. The Founding Committee members were legally elected and appointed to serve",
        "   their inaugural 12-month terms.",
        "",
        "The newly established Executive Committee is fully mandated to execute the",
        "duties outlined in the Constitution and commence daily operations of the UIP.",
        "",
        "This mandate supersedes all prior draft resolutions and acts as the foundational",
        "governing authority for the organization.",
        "",
        "",
        "Authorized By:",
        "___________________________",
        "Returning Officer / Chairman",
        "",
        f"Generated for System Testing: {datetime.date.today().isoformat()}"
    ]
    
    y = height - 160
    for line in text:
        c.drawString(70, y, line)
        y -= 20
        
    c.save()

create_founding_pdf('founding_members_mandate.pdf')
print("PDF generated successfully.")
