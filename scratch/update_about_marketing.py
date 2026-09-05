import re

with open('templates/program_sace/about.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_desc = """Welcome to the central portal for AIT's SACE-approved professional development activities. Whether you are a Teacher attending a live session, a Facilitator running the room, or a SACE Evaluator reviewing our compliance program, please click below to enter the Hub."""

new_desc = """Welcome to the central portal for AIT's SACE-approved professional development activities. 
<br><br>
<strong>For Teachers:</strong> Earn your CPTD points through our live, interactive workshops.
<br>
<strong>For Facilitators:</strong> Launch your own educational micro-franchise. Use our cutting-edge projector sync technology to uplift your community, run your own live workshops, and empower the next generation of learners.
<br><br>
Whether you are attending a session, leading a room, or reviewing our compliance program, click below to enter the Hub."""

content = content.replace(old_desc, new_desc)

with open('templates/program_sace/about.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated about.html with marketing pitch")
