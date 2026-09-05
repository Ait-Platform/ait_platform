import re

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('- [ ] 6. Interaction engine', '- [x] 6. Interaction engine')
text = text.replace('- [ ] 7. Task engine', '- [x] 7. Task engine')
text = text.replace('- [ ] 8. Reception dashboard', '- [x] 8. Reception dashboard')
text = text.replace('- [ ] 9. Manager dashboard', '- [x] 9. Manager dashboard')

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'w', encoding='utf-8') as f:
    f.write(text)
