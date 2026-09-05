import os
from PIL import Image, ImageDraw, ImageFont

# Create a blank white canvas matching the slide dimensions (assuming 16:9 like 1920x1080)
# Let's get the size of 1.png first
slide_dir = 'app/static/sace_slides'
first_slide = Image.open(os.path.join(slide_dir, '1.png'))
width, height = first_slide.size

cover = Image.new('RGB', (width, height), 'white')
draw = ImageDraw.Draw(cover)

# We might not have a fancy font installed, so we'll just try to draw text
try:
    font_large = ImageFont.truetype("arial.ttf", 100)
    font_medium = ImageFont.truetype("arial.ttf", 60)
except IOError:
    font_large = ImageFont.load_default()
    font_medium = ImageFont.load_default()

# Add text
title = "LITRE METHOD WORKSHOP GUIDE"
subtitle = "Participant Manual (P Guide)"
confidential = "CONFIDENTIAL - DO NOT DISTRIBUTE"

# Draw text centered (roughly)
draw.text((width//2 - 700, height//2 - 200), title, fill="black", font=font_large)
draw.text((width//2 - 400, height//2), subtitle, fill="#4f46e5", font=font_medium)
draw.text((width//2 - 450, height - 150), confidential, fill="red", font=font_medium)

images = [cover]
for i in range(1, 31):
    img_path = os.path.join(slide_dir, f"{i}.png")
    if os.path.exists(img_path):
        img = Image.open(img_path).convert('RGB')
        # resize to match cover if needed
        if img.size != cover.size:
            img = img.resize(cover.size, Image.Resampling.LANCZOS)
        images.append(img)

output_pdf = 'app/static/pdf/P_Guide.pdf'
images[0].save(
    output_pdf,
    save_all=True,
    append_images=images[1:],
    resolution=100.0,
    quality=85
)
print(f"Successfully generated {output_pdf} with Cover + 30 slides.")
