import bs4

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = bs4.BeautifulSoup(html, 'html.parser')

f_tab = soup.find(id='tab-f')
slides_container = f_tab.find('div', class_='flex-grow relative overflow-hidden')
all_slides = slides_container.find_all('div', class_=lambda c: c and 'slide-container' in c, recursive=False)

# Re-assign 5Problem to 5The Problem
img_4 = all_slides[5].find('img') # all_slides[0] is lobby, [1] is slide-0... [5] is slide-4
if img_4 and '5Problem.png' in img_4['src']:
    img_4['src'] = "{{ url_for('static', filename='sace_slides/5The Problem.png') }}"
    img_4['alt'] = "Slide 5: The Problem"

# Create new slide 5 (6Root Cause)
s5_html = """<div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center">
    <img alt="Slide 6: Root Cause" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/6Root Cause.png') }}"/>
</div>"""
s5 = bs4.BeautifulSoup(s5_html, 'html.parser').div

# Insert after slide-4 (all_slides[5])
all_slides[5].insert_after(s5)

# Refresh all_slides
all_slides = slides_container.find_all('div', class_=lambda c: c and 'slide-container' in c, recursive=False)

# Remove the old 3Intro.png slide (which was originally slide-8, now at index 10 since we added 1)
# Wait, let's just find it by img src to be safe
to_remove = None
for s in all_slides:
    img = s.find('img')
    if img and '3Intro.png' in img['src']:
        to_remove = s
        break

if to_remove:
    # Insert new slides 9, 10, 11 right before we remove it
    s_new_html = """
    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center">
        <img alt="Slide 7: Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/7Litre.png') }}"/>
    </div>
    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center">
        <img alt="Slide 8: Why Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/8Why Litre.png') }}"/>
    </div>
    <div class="slide-container absolute inset-0 hidden flex-col overflow-y-auto items-center justify-center">
        <img alt="Slide 9: What is Litre" class="max-h-[60vh] max-w-full object-contain mx-auto" src="{{ url_for('static', filename='sace_slides/9What is Litre.png') }}"/>
    </div>
    """
    s_new = bs4.BeautifulSoup(s_new_html, 'html.parser')
    for ns in s_new.find_all('div', recursive=False):
        to_remove.insert_before(ns)
    to_remove.extract()

# Re-number all slide IDs!
# all_slides[0] is lobby (id="slide-lobby")
# all_slides[1] is slide-0
all_slides = slides_container.find_all('div', class_=lambda c: c and 'slide-container' in c, recursive=False)
for i, s in enumerate(all_slides):
    if i == 0:
        s['id'] = 'slide-lobby'
    else:
        s['id'] = f'slide-{i-1}'

# Fix BS4 Jinja encoding
html_out = str(soup)
html_out = html_out.replace("%7B%7B%20", "{{ ").replace("%20%7D%7D", " }}")

with open('scratch/test_out2.html', 'w', encoding='utf-8') as f:
    f.write(html_out)

print(f"Total slides now: {len(all_slides)-1}")
