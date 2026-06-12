import sys

with open("app/subject_reading/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "READING_SUBJECT_ID" in line:
        print(f"Line {i+1}: {line.strip()}")
