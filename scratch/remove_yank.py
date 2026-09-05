import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove switchToParticipant from nextSlide, prevSlide, and startWorkshop
text = re.sub(r"if \(window\.parent && window\.parent !== window\) \{ window\.parent\.postMessage\(\{action: 'switchToParticipant'\}, '\*'\); \}", "", text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
