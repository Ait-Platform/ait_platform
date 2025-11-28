# app/scripts/blender.py
#
# Headless-safe builder for AIT "Adaptation Vector" ad.
# Designed to be run as:
#   blender.exe -b AIT_Adaptation_Vector.blend -P blender.py -- [args...]
#
# It will:
#   - Read CLI options passed after "--"
#   - Configure render settings (fps, frame range, output file)
#   - Apply a simple colour theme to the world
#   - Update text objects by name (if present)
#   - Optionally attach music and voice tracks in the VSE
#
# This script assumes your template .blend already has a basic scene
# configured (camera, animation, etc.). It does NOT create geometry
# from scratch.

import bpy
import os
import sys
import math
import argparse


# -------------------------------------------------------------------
# CLI parsing
# -------------------------------------------------------------------

def parse_ad_builder_args():
    """Parse arguments passed after -- from Blender."""
    if "--" not in sys.argv:
        return {}

    idx = sys.argv.index("--")
    cli_args = sys.argv[idx + 1 :]

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--title")
    parser.add_argument("--main_text")
    parser.add_argument("--sub_text")
    parser.add_argument("--theme")
    parser.add_argument("--fps", type=int)
    parser.add_argument("--frames", type=int)
    parser.add_argument("--mp4_out")
    parser.add_argument("--music")
    parser.add_argument("--voice")

    try:
        ns, _ = parser.parse_known_args(cli_args)
    except SystemExit:
        # Do not let argparse kill Blender
        return {}

    return {k: v for k, v in vars(ns).items() if v is not None}


CLI = parse_ad_builder_args()


# -------------------------------------------------------------------
# Defaults and config
# -------------------------------------------------------------------

# Base directory only used for fallback mp4_out; the UI should pass
# an explicit --mp4_out so this is just a safe default.
BASE_DIR = os.path.dirname(__file__)

AD_TITLE = CLI.get("title", "Adaptation Vector")
MAIN_TEXT = CLI.get(
    "main_text",
    "Have you lost a loved one? Measure your adaptation vector with AIT.",
)
SUB_TEXT = CLI.get(
    "sub_text",
    "Archoney Institute of Technology · Adaptation Vector",
)

THEME_KEY = CLI.get("theme", "navy")

FPS = CLI.get("fps", 30)
FRAME_END = CLI.get("frames", FPS * 30)

MP4_OUT = CLI.get("mp4_out", os.path.join(BASE_DIR, "ad.mp4"))

MUSIC_PATH = CLI.get("music", "")
VOICE_PATH = CLI.get("voice", "")


# -------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------

def safe_print(msg):
    """Print to console (and log file when called from Flask)."""
    try:
        print(msg)
    except Exception:
        # In very constrained environments just ignore logging errors.
        pass


def update_text_object(name, text):
    """Set the .body of a text object, if it exists and is a FONT object."""
    if not text:
        return

    obj = bpy.data.objects.get(name)
    if obj is None:
        safe_print(f"[ad_builder] Text object '{name}' not found; skipping.")
        return

    if obj.type != "FONT":
        safe_print(
            f"[ad_builder] Object '{name}' is not a FONT (type={obj.type}); skipping."
        )
        return

    obj.data.body = text
    safe_print(f"[ad_builder] Updated text on '{name}'.")


def ensure_sequence_editor(scene):
    """Ensure the scene has a sequence editor."""
    if scene.sequence_editor is None:
        scene.sequence_editor_create()
    return scene.sequence_editor


def clear_existing_audio_strips(seq, names=("Music", "Voice")):
    """Remove existing strips whose names match the given list."""
    to_remove = [
        s for s in seq.sequences_all if s.name in names and s.type == "SOUND"
    ]
    for s in to_remove:
        seq.sequences.remove(s)
        safe_print(f"[ad_builder] Removed existing audio strip '{s.name}'.")


def add_sound_strip(seq, path, name, channel):
    """Add a sound strip if the file exists."""
    if not path:
        safe_print(f"[ad_builder] No path for strip '{name}'; skipping.")
        return

    if not os.path.exists(path):
        safe_print(f"[ad_builder] File not found for strip '{name}': {path}")
        return

    try:
        strip = seq.sequences.new_sound(
            name=name,
            filepath=path,
            channel=channel,
            frame_start=1,
        )
        safe_print(f"[ad_builder] Added audio strip '{name}' from {path}")
        return strip
    except Exception as exc:
        safe_print(f"[ad_builder] Could not create sound strip '{name}': {exc}")


# -------------------------------------------------------------------
# Theme handling
# -------------------------------------------------------------------

def get_theme_colors(key):
    """Return a (r,g,b) triple for the world background based on theme."""
    # All colours are in 0-1 range.
    themes = {
        "navy": (0.02, 0.05, 0.12),   # dark navy
        "warm": (0.15, 0.07, 0.02),   # warm amber-ish
        "teal": (0.02, 0.10, 0.10),   # teal / calm
        "mono": (0.03, 0.03, 0.03),   # near-black
    }
    return themes.get(key, themes["navy"])


def apply_theme_to_world(scene, key):
    """Apply a simple colour theme to the world background."""
    world = scene.world
    if world is None:
        safe_print("[ad_builder] No world found on scene; creating one.")
        world = bpy.data.worlds.new("AIT_World")
        scene.world = world

    color = get_theme_colors(key)

    if world.use_nodes and world.node_tree:
        # Try to find background node in shader tree.
        bg_nodes = [
            n
            for n in world.node_tree.nodes
            if n.type == "BACKGROUND"
        ]
        if bg_nodes:
            bg_nodes[0].inputs[0].default_value = (color[0], color[1], color[2], 1.0)
            safe_print(f"[ad_builder] Applied theme '{key}' to world background (nodes).")
            return

    # Fallback: simple world colour.
    world.color = color
    safe_print(f"[ad_builder] Applied theme '{key}' to world.color.")


# -------------------------------------------------------------------
# Main configuration
# -------------------------------------------------------------------

def configure_scene():
    scene = bpy.context.scene

    # Render settings
    scene.render.fps = FPS
    scene.frame_start = 1
    scene.frame_end = FRAME_END
    scene.render.filepath = MP4_OUT

    # Sensible defaults for video output (adjust as needed)
    scene.render.resolution_x = 1920
    scene.render.resolution_y = 1080
    scene.render.resolution_percentage = 100

    # Use FFmpeg / H.264 if available
    scene.render.image_settings.file_format = "FFMPEG"
    scene.render.ffmpeg.format = "MPEG4"
    scene.render.ffmpeg.codec = "H264"
    scene.render.ffmpeg.constant_rate_factor = "HIGH"
    scene.render.ffmpeg.ffmpeg_preset = "GOOD"
    scene.render.ffmpeg.audio_codec = "AAC"

    safe_print(
        f"[ad_builder] Scene configured: fps={FPS}, "
        f"frame_end={FRAME_END}, filepath='{MP4_OUT}'"
    )

    # Apply theme
    apply_theme_to_world(scene, THEME_KEY)

    # Update text objects if the template contains them
    update_text_object("TitleText", AD_TITLE)
    update_text_object("MainText", MAIN_TEXT)
    update_text_object("SubText", SUB_TEXT)

    # Audio
    seq = ensure_sequence_editor(scene)
    clear_existing_audio_strips(seq, names=("Music", "Voice"))

    if MUSIC_PATH:
        add_sound_strip(seq, MUSIC_PATH, name="Music", channel=1)
    if VOICE_PATH:
        add_sound_strip(seq, VOICE_PATH, name="Voice", channel=2)

    return scene


def render_animation(scene):
    safe_print("[ad_builder] Starting render...")
    bpy.ops.render.render(animation=True)
    safe_print("[ad_builder] Render finished.")


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

def main():
    safe_print("[ad_builder] Starting AIT Ad Builder script.")
    safe_print(f"[ad_builder] CLI args: {CLI}")
    scene = configure_scene()
    render_animation(scene)
    safe_print("[ad_builder] AIT Ad Builder script complete.")


if __name__ == "__main__":
    main()
