from fpdf import FPDF

pdf = FPDF()
pdf.add_page()
pdf.set_font('Arial', 'B', 16)
pdf.cell(200, 10, txt='NATIONAL SENIOR CERTIFICATE', ln=True, align='C')
pdf.cell(200, 10, txt='GRADE 12', ln=True, align='C')
pdf.cell(200, 10, txt='MATHEMATICS P1', ln=True, align='C')
pdf.cell(200, 10, txt='NOVEMBER 2024', ln=True, align='C')

pdf.set_font('Arial', '', 12)
pdf.cell(200, 10, txt='MARKS: 150', ln=True)
pdf.cell(200, 10, txt='TIME: 3 hours', ln=True)
pdf.cell(200, 10, txt='', ln=True)

pdf.set_font('Arial', 'B', 12)
pdf.cell(200, 10, txt='QUESTION 1: ALGEBRA', ln=True)
pdf.set_font('Arial', '', 12)
pdf.cell(200, 10, txt='1.1 Solve for x:', ln=True)
pdf.cell(200, 10, txt='    1.1.1  x^2 - x - 20 = 0', ln=True)
pdf.cell(200, 10, txt='    1.1.2  2x^2 - 11x + 7 = 0 (correct to TWO decimal places)', ln=True)
pdf.cell(200, 10, txt='    1.1.3  5x^2 + 4 > 21x', ln=True)
pdf.cell(200, 10, txt='', ln=True)

pdf.set_font('Arial', 'B', 12)
pdf.cell(200, 10, txt='QUESTION 2: CALCULUS', ln=True)
pdf.set_font('Arial', '', 12)
pdf.cell(200, 10, txt="2.1 Determine f'(x) from first principles if f(x) = 3x^2 - 5", ln=True)
pdf.cell(200, 10, txt='2.2 Determine dy/dx if y = 2x^3 + 4/x', ln=True)
pdf.cell(200, 10, txt='', ln=True)

pdf.set_font('Arial', 'B', 12)
pdf.cell(200, 10, txt='QUESTION 3: FUNCTIONS', ln=True)
pdf.set_font('Arial', '', 12)
pdf.cell(200, 10, txt='3.1 Given f(x) = 2/(x-3) + 1. Write down equations of asymptotes.', ln=True)
pdf.cell(200, 10, txt='3.2 Determine the x and y intercepts of f.', ln=True)

pdf.output('app/data/dbe_papers/2024_NSC_Mathematics_P1_Mock.pdf')
print('Mock PDF created successfully.')
