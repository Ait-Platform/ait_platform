import re

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('- [ ] 18. Luna capabilities & Reporting', '- [x] 18. Luna capabilities & Reporting')
text = text.replace('- [ ] 19. Security hardening', '- [x] 19. Security hardening')
text = text.replace('- [ ] 20. Manor Gardens Pilot & Rollout', '- [x] 20. Manor Gardens Pilot & Rollout')

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'w', encoding='utf-8') as f:
    f.write(text.replace('\ufeff', ''))
