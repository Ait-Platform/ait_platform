with open("app/subject_reading/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_logic = r'video_src = url_for\("static", filename=f"uploads/reading_videos/\{lesson\[\'video_filename\'\]\}"\)'
new_logic = r'''domain = os.getenv("R2_PUBLIC_DOMAIN", "").rstrip("/")
    if domain:
        video_src = f"{domain}/reading_videos/{lesson['video_filename']}"
    else:
        video_src = url_for("static", filename=f"uploads/reading_videos/{lesson['video_filename']}")'''

text = re.sub(old_logic, new_logic, text)

# Add 'import os' if not present
if "import os" not in text:
    text = "import os\n" + text

with open("app/subject_reading/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated video_src to use Cloudflare R2")
