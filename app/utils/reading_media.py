"""R2-first Reading video delivery with the persistent disk preserved."""
import os
from pathlib import Path
from urllib.parse import quote, urlsplit

import requests


class ReadingMediaUnavailable(RuntimeError):
    pass


def reading_video_url(filename):
    """Use the existing public video bucket, preserving the exact object name."""
    if not filename or filename in {".", ".."} or any(c in filename for c in "/\\"):
        raise ReadingMediaUnavailable("Invalid Reading video filename.")
    domain = os.getenv("R2_PUBLIC_DOMAIN", "https://pub-d9878fa5cbe44074bd45bb83a4376153.r2.dev").strip().rstrip("/")
    parts = urlsplit(domain)
    if (parts.scheme != "https" or not parts.hostname or parts.username
            or parts.password or parts.query or parts.fragment):
        raise ReadingMediaUnavailable("R2_PUBLIC_DOMAIN must be a configured HTTPS URL.")
    prefix = os.getenv("R2_READING_PREFIX", "reading_videos").strip("/")
    if any(part in {".", ".."} for part in prefix.split("/")):
        raise ReadingMediaUnavailable("Invalid Reading video prefix.")
    key = "/".join(part for part in (prefix, filename) if part)
    return domain + "/" + quote(key, safe="/")


def verify_reading_video(url):
    """Check the remote object before recording that a lesson was served."""
    try:
        with requests.head(url, timeout=(5, 15), allow_redirects=False) as response:
            content_type = response.headers.get("Content-Type", "").split(";")[0].lower()
            if (response.status_code != 200
                    or int(response.headers.get("Content-Length", "0")) <= 0
                    or not (content_type.startswith("video/")
                            or content_type == "application/octet-stream")):
                raise ReadingMediaUnavailable("Reading video is missing or has invalid metadata in R2.")
    except (requests.RequestException, ValueError) as exc:
        raise ReadingMediaUnavailable("Could not verify the Reading video in R2.") from exc


def reading_video_disk_path(static_folder, filename):
    """Read-only fallback to the original persistent upload location."""
    if not filename or filename in {".", ".."} or any(c in filename for c in "/\\"):
        raise ReadingMediaUnavailable("Invalid Reading video filename.")
    root = (Path(static_folder) / "uploads/reading_videos").resolve()
    path = (root / filename).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ReadingMediaUnavailable("The disk fallback video is unavailable.")
    return path
