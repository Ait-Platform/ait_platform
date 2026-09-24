"""Verify all configured Reading videos in R2 without starting the Flask app.

Uses the current DATABASE_URL read-only. Optionally loads the local .env with
--dotenv. Reports HEAD and byte-range checks; never prints credentials.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

import requests
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("reading_media", ROOT / "app/utils/reading_media.py")
media = importlib.util.module_from_spec(spec)
spec.loader.exec_module(media)


def check_video(filename):
    url = media.reading_video_url(filename)
    media.verify_reading_video(url)
    result = {"filename": filename}
    for name, byte_range in (("start", "bytes=0-31"), ("end", "bytes=-32")):
        with requests.get(url, headers={"Range": byte_range}, timeout=(5, 20),
                          allow_redirects=False, stream=True) as response:
            content_range = response.headers.get("Content-Range", "")
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
            if response.status_code != 206 or not match:
                raise RuntimeError("R2 did not return a valid partial response for " + name)
            start, end, total = map(int, match.groups())
            body = response.raw.read(33)
            if len(body) != 32 or end - start != 31 or total < 32:
                raise RuntimeError("Unexpected byte-range size")
            if name == "start" and (start != 0 or b"ftyp" not in body):
                raise RuntimeError("The object does not have an MP4 header")
            if name == "end" and end != total - 1:
                raise RuntimeError("End-of-video seeking failed")
            result[name + "_range"] = "passed"
            result["bytes"] = total
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dotenv", action="store_true")
    parser.add_argument("--public-domain")
    parser.add_argument("--prefix", help="R2 object prefix; pass an empty string for bucket-root files")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.dotenv:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=True)
    if args.public_domain:
        os.environ["R2_PUBLIC_DOMAIN"] = args.public_domain
    if args.prefix is not None:
        os.environ["R2_READING_PREFIX"] = args.prefix
    if not os.getenv("DATABASE_URL"):
        parser.error("DATABASE_URL is not configured")
    if not os.getenv("R2_PUBLIC_DOMAIN"):
        parser.error("R2_PUBLIC_DOMAIN or --public-domain is required")
    engine = create_engine(os.environ["DATABASE_URL"], connect_args={"connect_timeout": 10})
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            database = conn.execute(text("SELECT current_database()")).scalar_one()
            lessons = list(conn.execute(text('SELECT id, "order", video_filename FROM rdp_lesson ORDER BY "order"')).mappings())
    except Exception as exc:
        print("Database verification failed (" + type(exc).__name__ + "); connection details suppressed.", file=sys.stderr)
        return 1
    finally:
        engine.dispose()
    results = []
    for lesson in lessons:
        result = {"lesson_id": lesson["id"], "filename": lesson["video_filename"]}
        try:
            result.update(check_video(lesson["video_filename"]))
            result["status"] = "passed"
        except Exception as exc:
            result.update(status="failed", error_type=type(exc).__name__)
            if isinstance(exc, (media.ReadingMediaUnavailable, RuntimeError)):
                result["reason"] = str(exc)
        results.append(result)
        print(json.dumps(result))
    report = {"database": database, "expected_lessons": 18, "actual_lessons": len(results),
              "passed": len(results) == 18 and all(r["status"] == "passed" for r in results),
              "results": results}
    if args.output:
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
