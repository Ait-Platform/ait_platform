import bs4

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = bs4.BeautifulSoup(html, 'html.parser')

f_tab = soup.find(id='tab-f')
slides_container = f_tab.find('div', class_='flex-grow relative overflow-hidden')
all_slides = slides_container.find_all('div', class_='slide-container', recursive=False)

# Let's see what is currently in slide-4, slide-5, etc.
for i, slide in enumerate(all_slides):
    h2 = slide.find('h2')
    img = slide.find('img')
    print(f"Slide {i-1}:")
    if h2:
        print(f"  H2: {h2.text}")
    if img:
        print(f"  IMG: {img.get('src')}")
    print("---")
