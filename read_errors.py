import json

with open("C:/Users/Sanjith/.gemini/antigravity/brain/0fc89fb1-3feb-482b-a3fe-93e89f7d1bf4/.system_generated/logs/transcript.jsonl", "r", encoding="utf-8") as f:
    lines = f.readlines()
    
with open("D:/Users/yeshk/Documents/ait_platform/found_errors.txt", "w", encoding="utf-8") as out:
    for line in lines[-200:]:
        data = json.loads(line)
        content = data.get("content", "")
        if "Setup Wizard Error" in content:
            out.write(content + "\n====================\n")
