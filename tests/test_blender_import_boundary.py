"""Standalone import regression: run with .venv/Scripts/python.exe -B this_file.

Uses real application modules, never the database-creating pytest fixtures.
No application factory, migration environment, or database connection is allowed.
"""
import builtins
from contextlib import ExitStack
import importlib
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class UnsafeOperation(BaseException):
    """Cannot be swallowed by application exception handlers."""


def forbid_operation(*args, **kwargs):
    raise UnsafeOperation("Application startup or database access is forbidden")


def guard_factory(frame, event, arg):
    if event == "call" and frame.f_code.co_name == "create_app":
        raise UnsafeOperation("create_app() must never run in this test")


class BlenderImportBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stack = ExitStack()
        cls.addClassCleanup(cls.stack.close)
        temporary = cls.stack.enter_context(tempfile.TemporaryDirectory())
        cls.stack.enter_context(patch.dict(os.environ, {
            "DATABASE_URL": "postgresql://test:test@127.0.0.1:1/import_only",
            "FLASK_INSTANCE_PATH": temporary,
            "SKIP_AUTO_MIGRATE": "1",
        }))
        original_import = builtins.__import__

        def without_bpy(name, *args, **kwargs):
            if name == "bpy" or name.startswith("bpy."):
                raise ModuleNotFoundError("bpy deliberately unavailable", name="bpy")
            return original_import(name, *args, **kwargs)

        cls.stack.enter_context(patch("builtins.__import__", without_bpy))
        cls.stack.enter_context(patch("socket.socket.connect", forbid_operation))
        cls.stack.enter_context(patch("sqlalchemy.engine.Engine.connect", forbid_operation))
        cls.stack.enter_context(patch("sqlalchemy.engine.Engine.raw_connection", forbid_operation))
        cls.stack.enter_context(patch("psycopg2.connect", forbid_operation))
        cls.stack.enter_context(patch("flask_migrate.upgrade", forbid_operation))
        previous_profile = sys.getprofile()
        cls.stack.callback(sys.setprofile, previous_profile)
        sys.setprofile(guard_factory)
        cls.stack.enter_context(patch.object(sys, "argv", ["import-boundary-test"]))
        cls.app = importlib.import_module("app")
        cls.builder = importlib.import_module("app.scripts.blender")

    def test_builder_import_and_defaults_without_bpy(self):
        self.assertEqual(self.builder.CLI, {})
        self.assertEqual(self.builder.AD_TITLE, "Adaptation Vector")
        self.assertEqual(self.builder.THEME_KEY, "navy")
        self.assertEqual(self.builder.FPS, 30)
        self.assertEqual(self.builder.FRAME_END, 900)
        self.assertEqual(self.builder.MP4_OUT, os.path.join(self.builder.BASE_DIR, "ad.mp4"))
        self.assertEqual((self.builder.MUSIC_PATH, self.builder.VOICE_PATH), ("", ""))

    def test_cli_parsing_and_overrides(self):
        overrides = {
            "title": "Test", "main_text": "Main", "sub_text": "Sub",
            "theme": "teal", "fps": 24, "frames": 48,
            "mp4_out": "test.mp4", "music": "music.wav", "voice": "voice.wav",
        }
        arguments = ["blender", "--"]
        for key, value in overrides.items():
            arguments.extend(["--" + key, str(value)])
        try:
            with patch.object(sys, "argv", arguments):
                importlib.reload(self.builder)
                self.assertEqual(self.builder.CLI, overrides)
                self.assertEqual(self.builder.AD_TITLE, "Test")
                self.assertEqual((self.builder.FPS, self.builder.FRAME_END), (24, 48))
                self.assertEqual(self.builder.MP4_OUT, "test.mp4")
        finally:
            importlib.reload(self.builder)
        self.assertEqual(self.builder.parse_ad_builder_args(), {})

    def test_config_and_database_modules_import_without_bpy(self):
        config = importlib.import_module("config")
        importlib.reload(config)
        self.assertIs(config.CLI, self.builder.CLI)
        self.assertEqual(config.AD_TITLE, "Adaptation Vector")
        self.assertIsNotNone(config.Config)
        extensions = importlib.import_module("app.extensions")
        self.assertIsNotNone(importlib.import_module("app.models"))
        self.assertGreater(len(extensions.db.metadata.tables), 0)
        self.assertTrue(callable(self.app.create_app))

    def test_blender_functions_require_bpy_only_when_called(self):
        calls = [
            (self.builder.update_text_object, ("TitleText", "Test")),
            (self.builder.apply_theme_to_world, (None, "navy")),
            (self.builder.configure_scene, ()),
            (self.builder.render_animation, (None,)),
        ]
        for function, arguments in calls:
            with self.subTest(function=function.__name__):
                with self.assertRaises(ModuleNotFoundError) as caught:
                    function(*arguments)
                self.assertEqual(caught.exception.name, "bpy")


if __name__ == "__main__":
    unittest.main(verbosity=2)
