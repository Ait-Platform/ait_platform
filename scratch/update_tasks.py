import re

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('- [ ] 2. Core organisation', '- [x] 2. Core organisation')
text = text.replace('- [ ] 3. Membership', '- [x] 3. Membership')
text = text.replace('- [ ] 4. Role/permission engine', '- [x] 4. Role/permission engine')
text = text.replace('- [ ] 5. Organisation context', '- [x] 5. Organisation context')

with open('C:/Users/Sanjith/.gemini/antigravity/brain/dcc17788-dfc9-47cd-bf94-3729fb287db0/task.md', 'w', encoding='utf-8') as f:
    f.write(text)
