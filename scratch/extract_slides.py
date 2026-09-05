import re

# Read old files
with open('scratch/old_f_board.html', 'r', encoding='utf-8') as f:
    f_html = f.read()
    
with open('scratch/old_p_board.html', 'r', encoding='utf-8') as f:
    p_html = f.read()

# Extract F slides (from <div id="slide-lobby" to the end of slide-18)
f_slides_match = re.search(r'(<div id="slide-lobby".*?)(<div id="roster-modal"|<script>)', f_html, re.DOTALL)
f_slides = f_slides_match.group(1) if f_slides_match else ""

# Ensure we remove the absolute inset-0 from f_slides if we want them to flow better, or keep them relative to a container.
# Wait, they use absolute inset-0. I'll put them in a relative container.

# Extract P views (from <div id="app-view-0" to the end of app-view-11)
p_views_match = re.search(r'(<div id="app-view-0".*?)(<script>|<!-- Roster Modal -->)', p_html, re.DOTALL)
p_views = p_views_match.group(1) if p_views_match else ""

# Write extracted blocks to temporary files so I can inspect them
with open('scratch/raw_f_slides.html', 'w', encoding='utf-8') as f:
    f.write(f_slides)
with open('scratch/raw_p_views.html', 'w', encoding='utf-8') as f:
    f.write(p_views)
