import asyncio
import pathlib
import sys
import edge_tts

# ---- Config (edit if you want a different voice/speed) ----
VOICE  = "en-US-AriaNeural"   # e.g. en-US-GuyNeural, en-GB-SoniaNeural
RATE   = "+0%"                # "-10%" slower, "+10%" faster
VOLUME = "+0%"                # "-5%" softer, "+5%" louder
# ------------------------------------------------------------

BASE     = pathlib.Path(__file__).parent
IN_FILE  = BASE / "tts_input.txt"
OUT_FILE = BASE / "tts_output.mp3"

async def synth():
    if not IN_FILE.exists():
        print(f"❌ Missing {IN_FILE}")
        sys.exit(1)

    text = IN_FILE.read_text(encoding="utf-8").strip()
    if not text:
        print("❌ tts_input.txt is empty")
        sys.exit(1)

    communicate = edge_tts.Communicate(text, VOICE, rate=RATE, volume=VOLUME)

    # Stream to mp3
    with open(OUT_FILE, "wb") as f:
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])

    print(f"✅ Wrote {OUT_FILE.resolve()}")

if __name__ == "__main__":
    try:
        asyncio.run(synth())
    except KeyboardInterrupt:
        pass
