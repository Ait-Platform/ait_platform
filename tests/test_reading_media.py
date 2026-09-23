"""Standalone R2 video tests; does not import or start the application."""
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("reading_media", Path(__file__).resolve().parents[1] / "app/utils/reading_media.py")
media = importlib.util.module_from_spec(spec)
spec.loader.exec_module(media)


class ReadingMediaTests(unittest.TestCase):
    def test_exact_key_and_url_encoding(self):
        with patch.dict(os.environ, {"R2_PUBLIC_DOMAIN": "https://assets.example.test/", "R2_READING_PREFIX": "reading_videos"}):
            self.assertEqual(media.reading_video_url("A B#.mp4"), "https://assets.example.test/reading_videos/A%20B%23.mp4")

    def test_flat_bucket_prefix(self):
        with patch.dict(os.environ, {"R2_PUBLIC_DOMAIN": "https://assets.example.test", "R2_READING_PREFIX": ""}):
            self.assertEqual(media.reading_video_url("lesson1.mp4"), "https://assets.example.test/lesson1.mp4")

    def test_invalid_configuration_and_paths(self):
        with patch.dict(os.environ, {"R2_PUBLIC_DOMAIN": ""}, clear=True):
            with self.assertRaises(media.ReadingMediaUnavailable):
                media.reading_video_url("lesson1.mp4")
        with patch.dict(os.environ, {"R2_PUBLIC_DOMAIN": "https://assets.example.test"}):
            for filename in ("", "../lesson.mp4", "folder/lesson.mp4", "folder\\lesson.mp4"):
                with self.subTest(filename=filename), self.assertRaises(media.ReadingMediaUnavailable):
                    media.reading_video_url(filename)

    def test_remote_errors_do_not_count_as_available(self):
        for status, size, content_type in ((404, "10", "video/mp4"), (200, "0", "video/mp4"), (200, "10", "text/html"), (302, "10", "video/mp4")):
            response = Mock(status_code=status, headers={"Content-Length": size, "Content-Type": content_type})
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            with patch.object(media.requests, "head", return_value=response), self.assertRaises(media.ReadingMediaUnavailable):
                media.verify_reading_video("https://assets.example.test/lesson1.mp4")

    def test_nonempty_video_passes(self):
        response = Mock(status_code=200, headers={"Content-Length": "100", "Content-Type": "video/mp4"})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(media.requests, "head", return_value=response):
            media.verify_reading_video("https://assets.example.test/lesson1.mp4")



    def test_disk_fallback_is_read_only_and_confined(self):
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as temp:
            root = Path(temp) / "uploads/reading_videos"
            root.mkdir(parents=True)
            video = root / "lesson1.mp4"
            video.write_bytes(b"master-video")
            self.assertEqual(media.reading_video_disk_path(temp, "lesson1.mp4"), video.resolve())
            self.assertEqual(video.read_bytes(), b"master-video")
            for name in ("missing.mp4", "../lesson1.mp4", "folder/lesson1.mp4"):
                with self.assertRaises(media.ReadingMediaUnavailable):
                    media.reading_video_disk_path(temp, name)


if __name__ == "__main__":
    unittest.main()
