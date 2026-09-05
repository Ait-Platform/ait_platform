import re

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('- [ ] 13. Providers', '- [x] 13. Providers')
text = text.replace('- [ ] 14. Municipality', '- [x] 14. Municipality')
text = text.replace('- [ ] 15. Governance', '- [x] 15. Governance')
text = text.replace('- [ ] 16. Documents', '- [x] 16. Documents')
text = text.replace('- [ ] 17. Communications', '- [x] 17. Communications')

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'w', encoding='utf-8') as f:
    f.write(text.replace('\ufeff', ''))
