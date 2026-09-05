import io

with io.open('scratch/old_f_board.html', 'r', encoding='utf-16le') as f:
    f_html = f.read()
    
with io.open('scratch/old_p_board.html', 'r', encoding='utf-16le') as f:
    p_html = f.read()

import re
f_slides_match = re.search(r'(<div id="slide-lobby".*?)(<div id="roster-modal"|<script>)', f_html, re.DOTALL)
f_slides = f_slides_match.group(1) if f_slides_match else ""

p_views_match = re.search(r'(<div id="app-view-0".*?)(<script>|<!-- Roster Modal -->)', p_html, re.DOTALL)
p_views = p_views_match.group(1) if p_views_match else ""

with io.open('scratch/raw_f_slides.html', 'w', encoding='utf-8') as f:
    f.write(f_slides)
with io.open('scratch/raw_p_views.html', 'w', encoding='utf-8') as f:
    f.write(p_views)
