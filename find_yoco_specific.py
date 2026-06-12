import json

log_path = r"C:\Users\Sanjith\.gemini\antigravity\brain\0fc89fb1-3feb-482b-a3fe-93e89f7d1bf4\.system_generated\logs\transcript.jsonl"
with open(log_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            entry = json.loads(line)
            content = entry.get("content", "")
            if "Bad Request" in content and "yoco" in content.lower():
                print(entry.get("created_at"))
                print(content[:500])
                print("-" * 20)
            if entry.get("type") == "PLANNER_RESPONSE" and "yoco" in content.lower() and "error" in content.lower():
                if "cancelUrl" in content or "localhost" in content or "127.0.0.1" in content:
                    print("PLANNER:", content[:500])
        except Exception:
            pass
