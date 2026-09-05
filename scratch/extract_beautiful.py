from bs4 import BeautifulSoup
import io

with io.open('scratch/old_f_board.html', 'r', encoding='utf-16le') as f:
    f_html = f.read()
    
with io.open('scratch/old_p_board.html', 'r', encoding='utf-16le') as f:
    p_html = f.read()

soup_f = BeautifulSoup(f_html, 'html.parser')
soup_p = BeautifulSoup(p_html, 'html.parser')

f_slides = []
for div in soup_f.find_all('div', class_='slide-container'):
    f_slides.append(str(div))
    
p_views = []
for div in soup_p.find_all('div', class_='app-view'):
    p_views.append(str(div))

f_html_clean = "\n".join(f_slides)
p_html_clean = "\n".join(p_views)

# Apply mockPoll fixes
p_html_clean = p_html_clean.replace("submitPoll('poll_crisis', 'FALSE', this)", "mockPoll('crisis')")
p_html_clean = p_html_clean.replace("submitPoll('poll_crisis', 'TRUE', this)", "mockPoll('crisis')")
p_html_clean = p_html_clean.replace("submitPoll('poll_root_cause', 'A', this)", "mockPoll('root')")
p_html_clean = p_html_clean.replace("submitPoll('poll_root_cause', 'B', this)", "mockPoll('root')")
p_html_clean = p_html_clean.replace("submitPoll('poll_root_cause', 'C', this)", "mockPoll('root')")
p_html_clean = p_html_clean.replace("submitPoll('poll_root_cause', 'D', this)", "mockPoll('root')")
p_html_clean = p_html_clean.replace("onclick=\"alert('Submitted!')\"", "onclick=\"mockPoll('generic')\"")
p_html_clean = p_html_clean.replace("submitAssessment()", "mockPoll('assessment')")

with io.open('scratch/raw_f_slides.html', 'w', encoding='utf-8') as f:
    f.write(f_html_clean)

with io.open('scratch/raw_p_views.html', 'w', encoding='utf-8') as f:
    f.write(p_html_clean)
