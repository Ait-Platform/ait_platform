import re

with open('AGENT.md', 'r', encoding='utf-8') as f:
    content = f.read()

new_rules = """

## 7. Template & UI Layout Rules
All templates must strictly follow this Tailwind format:
1. **Base Layout**: Must {% extends 'layout.html' %}.
2. **Tile Container**: All content must be inside a central tile/card (e.g., a white div with shadow and rounded corners).
3. **Color Strip**: The top of the tile must have a color strip matching the subject/welcome page color.
4. **Row 1 (Header)**: The title must be on the left, and a Back button on the right.
5. **Row 2 (Actions)**: Any other primary action buttons should be in row 2, right-aligned.
6. **Flash Messages**: Flash messages MUST be rendered *inside* the tile content area, not outside it.
7. **Forms & Textboxes**: All textboxes must have clear outlines (order border-slate-300) so the user sees where to type.
8. **Autofocus**: The cursor must automatically focus on the first textbox (utofocus attribute).
"""

content += new_rules

with open('AGENT.md', 'w', encoding='utf-8') as f:
    f.write(content)
