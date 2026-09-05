import os
from PIL import Image, ImageDraw, ImageFont

output_dir = 'app/static/pdf'
os.makedirs(output_dir, exist_ok=True)

def create_dummy_pdf(filename, title):
    img = Image.new('RGB', (1200, 1600), 'white')
    draw = ImageDraw.Draw(img)
    try:
        font_large = ImageFont.truetype("arial.ttf", 60)
    except:
        font_large = ImageFont.load_default()
    
    draw.text((100, 200), title, fill="black", font=font_large)
    draw.text((100, 300), "(Pending Final Document Upload)", fill="gray", font=font_large)
    
    img.save(os.path.join(output_dir, filename), resolution=100.0)

create_dummy_pdf('App_Form_1.pdf', 'SACE Application Form - Part 1')
create_dummy_pdf('App_Form_2.pdf', 'SACE Application Form - Part 2')
create_dummy_pdf('Facilitator_CVs.pdf', 'Facilitator CVs & Profiles')
print("Dummy PDFs generated.")
