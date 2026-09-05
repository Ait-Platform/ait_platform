import re
with open('C:\\Users\\Sanjith\\.gemini\\antigravity\\brain\\dcc17788-dfc9-47cd-bf94-3729fb287db0\\task.md', 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace("- [ ] **UIP-4.1", "- [x] **UIP-4.1")
with open('C:\\Users\\Sanjith\\.gemini\\antigravity\\brain\\dcc17788-dfc9-47cd-bf94-3729fb287db0\\task.md', 'w', encoding='utf-8') as f:
    f.write(text)
