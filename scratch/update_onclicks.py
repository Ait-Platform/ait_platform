import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace any alert(...) or submitLog(...) with nextStep()
text = re.sub(r'onclick="alert\(\'[^\']+\'\)"', 'onclick="nextStep()"', text)
text = re.sub(r'onclick="submitLog\(\'[^\']+\'\)"', 'onclick="nextStep()"', text)
text = re.sub(r"onclick=\"if\(document\.getElementById\('[^']+'\)\.checked\) \{ submitLog\('[^']+'\); \} else \{ alert\('[^']+'\); \}\"", 'onclick="nextStep()"', text)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
