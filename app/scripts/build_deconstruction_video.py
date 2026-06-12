# app/scripts/build_deconstruction_video.py
#
# Run as:
#   blender.exe -b -P build_deconstruction_video.py
#
# Flask passes in:
#   AIT_VIDEO_BASE_DIR  (folder with assets/output)
#   AIT_VIDEO_OUTPUT    (full path of MP4 to write)
#   AIT_VIDEO_MUSIC     (optional, full path)
#   AIT_VIDEO_VOICE     (optional, full path)

import os
import math
import bpy
from mathutils import Vector

# -----------------------------
# ENV CONFIG
# -----------------------------
BASE_DIR   = os.environ.get("AIT_VIDEO_BASE_DIR") or r"C:\Users\Sanjith\OneDrive\Documentos\LoloAd2025"
OUTPUT_MP4 = os.environ.get("AIT_VIDEO_OUTPUT") or os.path.join(BASE_DIR, "deconstruction.mp4")

MUSIC_OVERRIDE = os.environ.get("AIT_VIDEO_MUSIC")
VOICE_OVERRIDE = os.environ.get("AIT_VIDEO_VOICE")

FPS   = 30
NAVY  = (0.039, 0.106, 0.180, 1.0)
RED   = (0.913, 0.220, 0.173, 1.0)
WHITE = (1.0, 1.0, 1.0, 1.0)

# Timings (Frames) - 4 Slides, ~6 seconds each, plus Intro/Outro
INTRO_START, INTRO_END = 1, 120                   # 0 - 4s
SLIDE1_START, SLIDE1_END = 120, 300               # 4 - 10s
SLIDE2_START, SLIDE2_END = 300, 480               # 10 - 16s
SLIDE3_START, SLIDE3_END = 480, 660               # 16 - 22s
SLIDE4_START, SLIDE4_END = 660, 840               # 22 - 28s
OUTRO_START, OUTRO_END = 840, 1020                # 28 - 34s

F_END = OUTRO_END

# Copy
INTRO_TEXT = "The Art of Deconstruction"
SLIDE1_TITLE = "Rule 1: The Golden Rule"
SLIDE1_BODY = "Do not touch your calculator yet. Read the question fully."
SLIDE2_TITLE = "Rule 2: Action Verbs"
SLIDE2_BODY = "Determine = Number\nProve = Logic\nHence = Use Previous"
SLIDE3_TITLE = "Rule 3: The 'AND' Trap"
SLIDE3_BODY = "Look out for 'AND' or 'BOTH'. Don't miss the second part of a question!"
SLIDE4_TITLE = "Rule 4: The Formula Hunt"
SLIDE4_BODY = "See a keyword? Write the formula instantly in the margins."
OUTRO_TEXT = "Master the structure, master the math.\nait.mathwithhands.com"

# -----------------------------
# UTILS
# -----------------------------

def ensure_clean_start():
    bpy.ops.wm.read_factory_settings(use_empty=True)

def ensure_scene_main():
    sc = bpy.context.scene
    sc.name = "Main"
    sc.render.fps = FPS
    sc.frame_start = 1
    sc.frame_end = F_END
    sc.render.resolution_x = 1920
    sc.render.resolution_y = 1080
    sc.render.resolution_percentage = 100

    sc.view_settings.view_transform = 'Filmic'
    sc.view_settings.look = 'None'

    sc.render.image_settings.file_format = 'FFMPEG'
    sc.render.ffmpeg.format = 'MPEG4'
    sc.render.ffmpeg.codec = 'H264'
    sc.render.ffmpeg.constant_rate_factor = 'HIGH'
    sc.render.ffmpeg.audio_codec = 'AAC'
    sc.render.ffmpeg.audio_bitrate = 192
    sc.render.filepath = OUTPUT_MP4

    sc.sequence_editor_create()
    return sc

def add_color_strip(vse, name, color, channel, f_start, f_end):
    st = vse.sequences.new_effect(name=name, type='COLOR', channel=channel, frame_start=f_start, frame_end=f_end)
    st.color = color[:3]
    return st

def add_text_strip(vse, name, text, channel, f_start, f_end, size=0.08, x=0.5, y=0.5, color=WHITE):
    st = vse.sequences.new_effect(name=name, type='TEXT', channel=channel, frame_start=f_start, frame_end=f_end)
    st.text = text
    st.font_size = size
    st.color = color
    st.location = (x, y)
    st.wrap_width = 0.9
    if hasattr(st, "align_x"):
        st.align_x = 'CENTER'
    if hasattr(st, "align_y"):
        st.align_y = 'CENTER'
    st.blend_type = 'ALPHA_OVER'
    st.blend_alpha = 1.0
    return st

def add_sound_strip(vse, name, filepath, channel, f_start, f_end, volume=1.0):
    st = vse.sequences.new_sound(name=name, filepath=filepath, channel=channel, frame_start=f_start)
    st.frame_final_start = f_start
    st.frame_final_end   = f_end
    st.volume = volume
    return st

def key_opacity(st, frame, value):
    st.blend_alpha = value
    st.keyframe_insert(data_path="blend_alpha", frame=frame)

def build_slide(vse, title, body, channel_start, start, end):
    # Title
    t = add_text_strip(vse, f"{title}_title", title, channel=channel_start, f_start=start, f_end=end, size=0.1, x=0.5, y=0.65, color=RED)
    key_opacity(t, start, 0.0)
    key_opacity(t, start + 15, 1.0)
    key_opacity(t, end - 15, 1.0)
    key_opacity(t, end, 0.0)
    
    # Body
    b = add_text_strip(vse, f"{title}_body", body, channel=channel_start+1, f_start=start, f_end=end, size=0.08, x=0.5, y=0.45, color=WHITE)
    key_opacity(b, start + 10, 0.0)
    key_opacity(b, start + 25, 1.0)
    key_opacity(b, end - 15, 1.0)
    key_opacity(b, end, 0.0)

# -----------------------------
# MAIN BUILD
# -----------------------------
ensure_clean_start()
sc = ensure_scene_main()
vse = sc.sequence_editor

if os.path.isfile(OUTPUT_MP4):
    try: os.remove(OUTPUT_MP4)
    except: pass

# BG
add_color_strip(vse, "BG_Navy", NAVY, channel=1, f_start=1, f_end=F_END)

# Intro
intro = add_text_strip(vse, "Intro", INTRO_TEXT, channel=3, f_start=INTRO_START, f_end=INTRO_END, size=0.12, x=0.5, y=0.5)
key_opacity(intro, INTRO_START, 0.0)
key_opacity(intro, INTRO_START + 20, 1.0)
key_opacity(intro, INTRO_END - 20, 1.0)
key_opacity(intro, INTRO_END, 0.0)

# Slides
build_slide(vse, SLIDE1_TITLE, SLIDE1_BODY, 4, SLIDE1_START, SLIDE1_END)
build_slide(vse, SLIDE2_TITLE, SLIDE2_BODY, 4, SLIDE2_START, SLIDE2_END)
build_slide(vse, SLIDE3_TITLE, SLIDE3_BODY, 4, SLIDE3_START, SLIDE3_END)
build_slide(vse, SLIDE4_TITLE, SLIDE4_BODY, 4, SLIDE4_START, SLIDE4_END)

# Outro
outro = add_text_strip(vse, "Outro", OUTRO_TEXT, channel=3, f_start=OUTRO_START, f_end=OUTRO_END, size=0.09, x=0.5, y=0.5)
key_opacity(outro, OUTRO_START, 0.0)
key_opacity(outro, OUTRO_START + 20, 1.0)
key_opacity(outro, OUTRO_END - 20, 1.0)
key_opacity(outro, OUTRO_END, 0.0)

# Audio
if MUSIC_OVERRIDE and os.path.isfile(MUSIC_OVERRIDE):
    add_sound_strip(vse, "Music", MUSIC_OVERRIDE, channel=2, f_start=1, f_end=F_END, volume=0.5)

if VOICE_OVERRIDE and os.path.isfile(VOICE_OVERRIDE):
    add_sound_strip(vse, "VoiceOver", VOICE_OVERRIDE, channel=3, f_start=1, f_end=F_END, volume=1.0)

# Render
bpy.ops.render.render(animation=True, write_still=False)
print("✅ Rendered Deconstruction Video:", OUTPUT_MP4)
