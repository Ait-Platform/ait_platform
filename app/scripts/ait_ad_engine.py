# app/scripts/ait_ad_engine.py
#
# Run as:
#   blender.exe -b -P ait_ad_engine.py
#
# Flask passes in:
#   AIT_AD_BASE_DIR  (folder with assets/output)
#   AIT_AD_OUTPUT    (full path of MP4 to write)
#   AIT_AD_LOGO      (optional, full path)
#   AIT_AD_MUSIC     (optional, full path)
#   AIT_AD_VOICE     (optional, full path)

import os
import math
import bpy
from mathutils import Vector

# -----------------------------
# ENV CONFIG
# -----------------------------
BASE_DIR   = os.environ.get("AIT_AD_BASE_DIR") or r"C:\Users\Sanjith\OneDrive\Documentos\LoloAd2025"
OUTPUT_MP4 = os.environ.get("AIT_AD_OUTPUT") or os.path.join(BASE_DIR, "final_ad.mp4")

LOGO_OVERRIDE  = os.environ.get("AIT_AD_LOGO")
MUSIC_OVERRIDE = os.environ.get("AIT_AD_MUSIC")
VOICE_OVERRIDE = os.environ.get("AIT_AD_VOICE")
QR_OVERRIDE    = os.environ.get("AIT_AD_QR")     # full path or None


FPS   = 30
SEC   = 30
F_END = FPS * SEC

NAVY  = (0.039, 0.106, 0.180, 1.0)
RED   = (0.913, 0.220, 0.173, 1.0)
WHITE = (1.0, 1.0, 1.0, 1.0)

OPEN_START, OPEN_END       = 1, 150
LINE2_START, LINE2_END     = 160, 300
#INF_START, INF_END         = 300, 540
INF_START, INF_END         = FPS, F_END

MID_A_START, MID_A_END     = 540, 610
MID_B_START, MID_B_END     = 610, 680
MID_C_START, MID_C_END     = 680, 750
ENDCARD_START, ENDCARD_END = 750, F_END

# Copy (fixed for now)
HOOK_TEXT  = "Have you lost a loved one?"
LINE2_TEXT = (
    "At the Archoney Institute of Technology, we measure what changes inside you — "
    "your Adaptation Vector."
)
MID_A_TEXT = "Discover how you grow through change."
MID_B_TEXT = "Learn who you’re becoming."
MID_C_TEXT = "Measure your Adaptation Vector"
END_FOOTER = "Archoney Institute of Technology — ait.mathwithhands.com"

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

def find_media():
    """
    Only use explicitly provided overrides; no auto-detect from BASE_DIR.

    Returns:
      (logo_path or None, vo_path or None, music_path or None)
    """
    logo = LOGO_OVERRIDE if LOGO_OVERRIDE and os.path.isfile(LOGO_OVERRIDE) else None
    vo = VOICE_OVERRIDE if VOICE_OVERRIDE and os.path.isfile(VOICE_OVERRIDE) else None
    music = MUSIC_OVERRIDE if MUSIC_OVERRIDE and os.path.isfile(MUSIC_OVERRIDE) else None
    return logo, vo, music

def add_color_strip(vse, name, color, channel, f_start, f_end):
    st = vse.sequences.new_effect(
        name=name,
        type='COLOR',
        channel=channel,
        frame_start=f_start,
        frame_end=f_end,
    )
    st.color = color[:3]
    return st

def add_text_strip(vse, name, text, channel, f_start, f_end,
                   size=0.08, x=0.5, y=0.5, color=(1,1,1,1)):
    st = vse.sequences.new_effect(
        name=name,
        type='TEXT',
        channel=channel,
        frame_start=f_start,
        frame_end=f_end,
    )
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

def add_image_strip(vse, name, filepath, channel, f_start, f_end):
    st = vse.sequences.new_image(
        name=name,
        filepath=filepath,
        channel=channel,
        frame_start=f_start,
    )
    st.frame_final_start = f_start
    st.frame_final_end   = f_end
    return st

def add_sound_strip(vse, name, filepath, channel, f_start, f_end, volume=1.0):
    st = vse.sequences.new_sound(
        name=name,
        filepath=filepath,
        channel=channel,
        frame_start=f_start,
    )
    st.frame_final_start = f_start
    st.frame_final_end   = f_end
    st.volume = volume
    return st

def key_opacity(st, frame, value):
    st.blend_alpha = value
    st.keyframe_insert(data_path="blend_alpha", frame=frame)

def key_volume(st, frame, vol):
    st.volume = vol
    st.keyframe_insert(data_path="volume", frame=frame)

def build_endcard(vse, start, end):
    cta = add_text_strip(
        vse, "CTA", "Measure your Adaptation Vector",
        channel=7, f_start=start, f_end=end,
        size=0.11, x=0.5, y=0.58
    )
    cta.wrap_width = 0.8
    key_opacity(cta, start, 0.0)
    key_opacity(cta, start + 10, 1.0)
    key_opacity(cta, end - 1, 1.0)

    footer = add_text_strip(
        vse, "Footer", END_FOOTER,
        channel=7, f_start=start + 5, f_end=end,
        size=0.05, x=0.5, y=0.45
    )
    footer.wrap_width = 0.8
    key_opacity(footer, start + 5, 0.0)
    key_opacity(footer, start + 15, 1.0)
    key_opacity(footer, end - 1, 1.0)

# -----------------------------
# MAIN BUILD
# -----------------------------

ensure_clean_start()
sc = ensure_scene_main()
vse = sc.sequence_editor

logo_path, vo_path, music_path = find_media()
# QR (optional, from env)
qr_path = QR_OVERRIDE if QR_OVERRIDE and os.path.isfile(QR_OVERRIDE) else None

# Clear existing MP4 if present
if os.path.isfile(OUTPUT_MP4):
    try:
        os.remove(OUTPUT_MP4)
    except Exception as exc:
        print("[AIT-AD] Could not remove old MP4:", exc)

# Background
add_color_strip(vse, "BG_Navy", NAVY, channel=1, f_start=1, f_end=F_END)

# Hook
t1 = add_text_strip(
    vse, "Hook", HOOK_TEXT,
    channel=3,
    f_start=OPEN_START,
    f_end=OPEN_END,
    size=0.105,
    x=0.5,
    y=0.58,
)
t1.wrap_width = 0.8
key_opacity(t1, OPEN_START, 0.0)
key_opacity(t1, OPEN_START + 10, 1.0)
key_opacity(t1, OPEN_END - 20, 1.0)
key_opacity(t1, OPEN_END, 0.0)

# Line 2
t2 = add_text_strip(
    vse, "Line2", LINE2_TEXT,
    channel=3,
    f_start=LINE2_START,
    f_end=LINE2_END,
    size=0.085,
    x=0.5,
    y=0.50,
)
t2.wrap_width = 0.8
key_opacity(t2, LINE2_START, 0.0)
key_opacity(t2, LINE2_START + 10, 1.0)
key_opacity(t2, LINE2_END - 10, 1.0)
key_opacity(t2, LINE2_END, 0.0)

# Middles
midA = add_text_strip(
    vse, "MidA", MID_A_TEXT,
    channel=3,
    f_start=MID_A_START,
    f_end=MID_A_END,
    size=0.085,
    x=0.5,
    y=0.53,
)
midA.wrap_width = 0.8
key_opacity(midA, MID_A_START, 0.0)
key_opacity(midA, MID_A_START + 8, 1.0)
key_opacity(midA, MID_A_END - 8, 1.0)
key_opacity(midA, MID_A_END, 0.0)

midB = add_text_strip(
    vse, "MidB", MID_B_TEXT,
    channel=3,
    f_start=MID_B_START,
    f_end=MID_B_END,
    size=0.085,
    x=0.5,
    y=0.49,
)
midB.wrap_width = 0.8
key_opacity(midB, MID_B_START, 0.0)
key_opacity(midB, MID_B_START + 8, 1.0)
key_opacity(midB, MID_B_END - 8, 1.0)
key_opacity(midB, MID_B_END, 0.0)

midC = add_text_strip(
    vse, "MidC", MID_C_TEXT,
    channel=6,
    f_start=MID_C_START,
    f_end=MID_C_END,
    size=0.095,
    x=0.5,
    y=0.56,
)
midC.wrap_width = 0.8
key_opacity(midC, MID_C_START, 0.0)
key_opacity(midC, MID_C_START + 8, 1.0)
key_opacity(midC, MID_C_END - 8, 1.0)
key_opacity(midC, MID_C_END, 0.0)

# End card
build_endcard(vse, ENDCARD_START, ENDCARD_END)
# QR overlay on endcard (bottom-right), if provided
if qr_path:
    qr_strip = add_image_strip(
        vse,
        "QR_Code",
        qr_path,
        channel=9,  # above text and BG
        f_start=ENDCARD_START,
        f_end=F_END,
    )
    # Try to move & scale the QR into bottom-right corner
    try:
        # These properties exist on Blender 3.0+; wrap in try to be safe.
        t = qr_strip.transform
        t.scale_x = 0.25
        t.scale_y = 0.25
        # offset in normalized-ish units; tweak if needed
        t.offset_x = 0.35   # move to the right
        t.offset_y = -0.25  # move down
    except AttributeError:
        # If transform is not available, just leave it in default position
        pass

# Logo
if logo_path:
    logo = add_image_strip(
        vse, "AIT_Logo", logo_path,
        channel=4,
        f_start=OPEN_END,
        f_end=LINE2_END,
    )
    logo.blend_alpha = 0.0
    logo.keyframe_insert(data_path="blend_alpha", frame=OPEN_END)
    logo.blend_alpha = 1.0
    logo.keyframe_insert(data_path="blend_alpha", frame=OPEN_END + 10)
    logo.blend_alpha = 1.0
    logo.keyframe_insert(data_path="blend_alpha", frame=LINE2_END - 10)
    logo.blend_alpha = 0.0
    logo.keyframe_insert(data_path="blend_alpha", frame=LINE2_END)

# Audio
if music_path:
    music = add_sound_strip(
        vse, "Music", music_path,
        channel=1,
        f_start=1,
        f_end=F_END,
        volume=0.6,
    )
    key_volume(music, 1, 0.55)
    key_volume(music, ENDCARD_END - 60, 0.55)
    key_volume(music, ENDCARD_END - 60, 0.75)
    key_volume(music, ENDCARD_END, 0.75)

if vo_path:
    vo = add_sound_strip(
        vse, "VoiceOver", vo_path,
        channel=2,
        f_start=1,
        f_end=F_END,
        volume=1.0,
    )

# Infinity scene (same as before, shortened a bit)
def build_infinity_scene():
    sc_inf = bpy.data.scenes.new("Infinity")
    sc_inf.render.resolution_x = 1920
    sc_inf.render.resolution_y = 1080
    sc_inf.render.fps = FPS
    sc_inf.frame_start = INF_START
    sc_inf.frame_end = INF_END
    sc_inf.view_settings.view_transform = 'Filmic'
    sc_inf.view_settings.look = 'None'

    world = bpy.data.worlds.new("World_Inf")
    world.color = (NAVY[0]*0.6, NAVY[1]*0.6, NAVY[2]*0.6)
    sc_inf.world = world

    col = bpy.data.collections.new("InfinityCol")
    sc_inf.collection.children.link(col)

    cam_data = bpy.data.cameras.new("InfCam")
    cam = bpy.data.objects.new("InfCam", cam_data)
    cam.location = (0.0, -3.0, 0.5)
    cam.rotation_euler = (math.radians(90), 0.0, 0.0)
    col.objects.link(cam)
    sc_inf.camera = cam

    light_data = bpy.data.lights.new("Fill", 'AREA')
    light_data.energy = 500
    light = bpy.data.objects.new("Fill", light_data)
    light.location = (0.0, -2.0, 2.0)
    col.objects.link(light)

    curve_data = bpy.data.curves.new('InfCurve', type='CURVE')
    curve_data.dimensions = '3D'
    spline = curve_data.splines.new('BEZIER')
    spline.bezier_points.add(7)

    pts = [
        (-1.2, 0.0, 0.0),
        (-0.8, 0.6, 0.0),
        (0.0, 0.4, 0.0),
        (0.8, 0.6, 0.0),
        (1.2, 0.0, 0.0),
        (0.8, -0.6, 0.0),
        (0.0, -0.4, 0.0),
        (-0.8, -0.6, 0.0),
    ]
    for i, p in enumerate(spline.bezier_points):
        p.co = Vector(pts[i])
        p.handle_left_type = 'AUTO'
        p.handle_right_type = 'AUTO'

    curve_obj = bpy.data.objects.new('InfinityCurve', curve_data)
    col.objects.link(curve_obj)

    curve_data.bevel_depth = 0.02
    curve_data.bevel_resolution = 8

     # Emission material with rainbow gradient along the curve
    mat = bpy.data.materials.new("InfMat")
    mat.use_nodes = True
    nodes = mat.node_tree.nodes
    links = mat.node_tree.links

    # Clear everything except the output
    for n in list(nodes):
        if n.type != 'OUTPUT_MATERIAL':
            nodes.remove(n)
    out = nodes.get("Material Output")

    # Texture coordinates -> Separate XYZ -> ColorRamp -> Emission -> Output
    tex = nodes.new("ShaderNodeTexCoord")
    sep = nodes.new("ShaderNodeSeparateXYZ")
    ramp = nodes.new("ShaderNodeValToRGB")
    emis = nodes.new("ShaderNodeEmission")

    # A few rainbow-ish colors across X
    ramp.color_ramp.elements[0].position = 0.0
    ramp.color_ramp.elements[0].color = (1.0, 0.0, 0.0, 1.0)   # red

    e1 = ramp.color_ramp.elements.new(0.25)
    e1.color = (1.0, 1.0, 0.0, 1.0)   # yellow

    e2 = ramp.color_ramp.elements.new(0.5)
    e2.color = (0.0, 1.0, 0.0, 1.0)   # green

    e3 = ramp.color_ramp.elements.new(0.75)
    e3.color = (0.0, 0.0, 1.0, 1.0)   # blue

    ramp.color_ramp.elements[1].position = 1.0
    ramp.color_ramp.elements[1].color = (0.8, 0.0, 1.0, 1.0)   # magenta

    emis.inputs["Strength"].default_value = 2.5

    # Wire up the nodes
    links.new(tex.outputs["Object"], sep.inputs["Vector"])
    links.new(sep.outputs["X"], ramp.inputs["Fac"])
    links.new(ramp.outputs["Color"], emis.inputs["Color"])
    links.new(emis.outputs["Emission"], out.inputs["Surface"])

    curve_obj.data.materials.append(mat)


    try:
        curve_data.bevel_factor_start = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_START)
        curve_data.bevel_factor_end = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_START)

        curve_data.bevel_factor_start = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_START + 120)
        curve_data.bevel_factor_end = 1.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_START + 120)

        curve_data.bevel_factor_start = 0.15
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_END)
        curve_data.bevel_factor_end = 1.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_END)
    except Exception as exc:
        print("[AIT-AD] Bevel animation error:", exc)

    return sc_inf

sc_inf = build_infinity_scene()
inf_strip = vse.sequences.new_scene(
    name="InfinityScene",
    scene=sc_inf,
    channel=8,
    frame_start=INF_START,
)
inf_strip.frame_final_start = INF_START
inf_strip.frame_final_end   = INF_END

# -----------------------------
# RENDER MP4 ONLY
# -----------------------------
bpy.ops.render.render(animation=True, write_still=False)
print("✅ Rendered MP4:", OUTPUT_MP4)
