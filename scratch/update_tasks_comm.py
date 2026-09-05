import re

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('- [ ] 10. Credit ledger', '- [x] 10. Credit ledger')
text = text.replace('- [ ] 11. Usage metering', '- [x] 11. Usage metering')
text = text.replace('- [ ] 12. AI Gateway', '- [x] 12. AI Gateway')

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'w', encoding='utf-8') as f:
    f.write(text.replace('\ufeff', ''))
