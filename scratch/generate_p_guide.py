import os
from PIL import Image

slide_dir = 'app/static/sace_slides'
output_dir = 'app/static/pdf'
os.makedirs(output_dir, exist_ok=True)
output_pdf = os.path.join(output_dir, 'P_Guide.pdf')

images = []
for i in range(1, 31):
    img_path = os.path.join(slide_dir, f"{i}.png")
    if os.path.exists(img_path):
        # Open and convert to RGB (required for saving as PDF)
        img = Image.open(img_path).convert('RGB')
        images.append(img)
    else:
        print(f"Missing slide: {img_path}")

if images:
    images[0].save(
        output_pdf,
        save_all=True,
        append_images=images[1:],
        resolution=100.0,
        quality=90
    )
    print(f"Successfully generated {output_pdf} with {len(images)} slides.")
else:
    print("No slides found!")
