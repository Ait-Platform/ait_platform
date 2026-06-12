import json

log_path = r"C:\Users\Sanjith\.gemini\antigravity\brain\0fc89fb1-3feb-482b-a3fe-93e89f7d1bf4\.system_generated\logs\transcript.jsonl"
with open(log_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            entry = json.loads(line)
            if entry.get("type") == "RUN_COMMAND":
                content = entry.get("content", "")
                if "yoco" in content.lower() and "error" in content.lower():
                    if "api.yoco.com" in content or "payments.yoco.com" in content:
                        print("-" * 50)
                        print(entry.get("created_at"))
                        print(content)
        except Exception:
            pass
