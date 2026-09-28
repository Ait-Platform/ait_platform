from fpdf import FPDF
from datetime import date

pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", size=12)

# Letterhead
pdf.set_font("Arial", 'B', 16)
pdf.cell(200, 10, txt="ETHEKWINI MUNICIPALITY", ln=1, align='C')
pdf.set_font("Arial", 'B', 14)
pdf.cell(200, 10, txt="Office of the City Manager", ln=1, align='C')
pdf.ln(10)

# Date & Reference
pdf.set_font("Arial", size=12)
pdf.cell(200, 10, txt=f"Date: {date.today().strftime('%B %d, %Y')}", ln=1, align='R')
pdf.cell(200, 10, txt="Ref: MO-APPT-2026", ln=1, align='R')
pdf.ln(10)

# Body
pdf.set_font("Arial", 'B', 12)
pdf.cell(200, 10, txt="SUBJECT: OFFICIAL APPOINTMENT OF MUNICIPAL OFFICER", ln=1, align='L')
pdf.ln(5)

pdf.set_font("Arial", size=12)
body = (
    "To the Secretary of the Manor Gardens UIP,\n\n"
    "This letter serves as the official mandate and notification from the eThekwini Municipality "
    "that the bearer of this document is officially appointed as the Municipal Officer (MO) "
    "for the Manor Gardens Urban Improvement Precinct (UIP).\n\n"
    "By virtue of this mandate, the MO is authorized to act as the primary liaison between "
    "the Municipality and the UIP, ensuring compliance with municipal bylaws, facilitating "
    "service delivery, and participating in the UIP's governance processes.\n\n"
    "Please update your committee records and grant the necessary system access to reflect "
    "this official appointment immediately."
)
pdf.multi_cell(0, 10, txt=body)
pdf.ln(15)

# Signoff
pdf.cell(200, 10, txt="Sincerely,", ln=1, align='L')
pdf.ln(10)
pdf.set_font("Arial", 'B', 12)
pdf.cell(200, 10, txt="[Signature]", ln=1, align='L')
pdf.cell(200, 10, txt="City Manager, eThekwini Municipality", ln=1, align='L')
pdf.cell(200, 10, txt="Official Municipal Stamp Applied", ln=1, align='L')

pdf.output("mo_appointment_letter.pdf")
print("PDF created.")
