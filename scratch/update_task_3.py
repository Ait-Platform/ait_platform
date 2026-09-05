import re
with open('C:\\Users\\Sanjith\\.gemini\\antigravity\\brain\\dcc17788-dfc9-47cd-bf94-3729fb287db0\\task.md', 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace("- [ ] **PHASE 3", "- [x] **PHASE 3")
with open('C:\\Users\\Sanjith\\.gemini\\antigravity\\brain\\dcc17788-dfc9-47cd-bf94-3729fb287db0\\task.md', 'w', encoding='utf-8') as f:
    f.write(text)
