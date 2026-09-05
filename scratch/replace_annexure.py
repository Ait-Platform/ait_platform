import re

with open('templates/program_sace/compliance/annexure_b.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = """
                        <li><strong>Slide 8 (The English Family):</strong> App Verification: The app prompts a discussion: "How will you adapt this story for your specific classroom demographic?".</li>
                        <li><strong>Slide 9 (Introducing the Vowels):</strong> Directing focus to the projector for vowel foundation (A, E, I, O, U).</li>
                        <li><strong>Slide 10 (The LiTRE Blending Machine):</strong> The Palm as the Meeting Place. App Verification: A digital checklist of the vowels where teachers pair up and grade each other's pronunciation.</li>
                        <li><strong>Slide 11 (Practice Round):</strong> Collective practice round utilizing the palm method.</li>
"""

pattern = re.compile(r"<li><strong>Slides 8 & 9 \(Case Study\):.*?<li><strong>Slide 11 \(Draw the Palm - Practical 1\):.*?evidence\.</li>", re.DOTALL)
content = pattern.sub(replacement.strip(), content)

with open('templates/program_sace/compliance/annexure_b.html', 'w', encoding='utf-8') as f:
    f.write(content)
