import json

log_path = r"C:\Users\Sanjith\.gemini\antigravity\brain\0fc89fb1-3feb-482b-a3fe-93e89f7d1bf4\.system_generated\logs\transcript.jsonl"
with open(log_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            entry = json.loads(line)
            if entry.get("type") == "USER_INPUT":
                content = entry.get("content", "").lower()
                if "yoco" in content or "error" in content:
                    print("-" * 50)
                    print(entry.get("created_at"))
                    print(entry.get("content"))
        except Exception:
            pass
