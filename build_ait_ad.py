# Blender 4.x — Headless-safe builder for AIT “Adaptation Vector” 30s ad
# No UI operators; uses data API so it runs in --background from VS Code/Terminal.

import bpy, os, math
from mathutils import Vector

# -----------------------------
# CONFIG
# -----------------------------
BASE_DIR = r"C:\Users\Sanjith\OneDrive\Documentos\LoloAd2025"
BLEND_OUT = os.path.join(BASE_DIR, "AIT_Adaptation_Vector.blend")
MP4_OUT   = os.path.join(BASE_DIR, "final_ad.mp4")

FPS = 30
SEC = 30
F_END = FPS * SEC

# Brand colors (approx. linear)
NAVY  = (0.039, 0.106, 0.180, 1.0)   # #0A1B2E
RED   = (0.913, 0.220, 0.173, 1.0)   # #E9382C
WHITE = (1.0, 1.0, 1.0, 1.0)

# Timing (frames)
OPEN_START, OPEN_END = 1, 150         # 0–5s
LINE2_START, LINE2_END = 160, 300     # 5.3–10s
INF_START, INF_END = 300, 540         # 10–18s
MID_A_START, MID_A_END = 540, 610     # 18–20.3s
MID_B_START, MID_B_END = 610, 680     # 20.3–22.6s
MID_C_START, MID_C_END = 680, 750     # 22.6–25s
ENDCARD_START, ENDCARD_END = 750, F_END

# Copy (can edit here)
HOOK_TEXT = "Have you lost a loved one?"
LINE2_TEXT = ("At the Archoney Institute of Technology, we measure what changes inside you — "
              "your Adaptation Vector.")
MID_A_TEXT = "Discover how you grow through change."
MID_B_TEXT = "Learn who you’re becoming."
MID_C_TEXT = "Measure your Adaptation Vector"
END_FOOTER = "Archoney Institute of Technology — ait.mathwithhands.com"

# -----------------------------
# UTILS
# -----------------------------

def build_endcard(vse, start, end):
    # CTA headline
    cta = add_text_strip(
        vse, "CTA", "Measure your Adaptation Vector",
        channel=7, f_start=start, f_end=end,
        size=0.11, x=0.5, y=0.58
    )
    cta.wrap_width = 0.8
    # fade in then HOLD to end
    key_opacity(cta, start, 0.0)
    key_opacity(cta, start + 10, 1.0)
    key_opacity(cta, end - 1, 1.0)

    # Footer
    footer = add_text_strip(
        vse, "Footer",
        "Archoney Institute of Technology — ait.mathwithhands.com",
        channel=7, f_start=start + 5, f_end=end,
        size=0.05, x=0.5, y=0.45
    )
    footer.wrap_width = 0.8
    key_opacity(footer, start + 5, 0.0)
    key_opacity(footer, start + 15, 1.0)
    key_opacity(footer, end - 1, 1.0)

def ensure_clean_start():
    # start from a clean file to avoid conflicts
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

    # Filmic tone mapping
    sc.view_settings.view_transform = 'Filmic'
    sc.view_settings.look = 'None'

    # Output settings
    sc.render.image_settings.file_format = 'FFMPEG'
    sc.render.ffmpeg.format = 'MPEG4'
    sc.render.ffmpeg.codec = 'H264'
    sc.render.ffmpeg.constant_rate_factor = 'HIGH'
    sc.render.ffmpeg.audio_codec = 'AAC'
    sc.render.ffmpeg.audio_bitrate = 192
    sc.render.filepath = MP4_OUT

    sc.sequence_editor_create()
    return sc

def find_media():
    logo, vo, music = None, None, None
    if not os.path.isdir(BASE_DIR): return logo, vo, music
    imgs, auds = [], []
    for fn in os.listdir(BASE_DIR):
        p = os.path.join(BASE_DIR, fn)
        if not os.path.isfile(p): continue
        lo = fn.lower()
        if lo.endswith((".png",".jpg",".jpeg")):
            imgs.append(p)
        if lo.endswith((".wav",".mp3",".flac",".m4a",".aac",".ogg")):
            auds.append(p)
    # Pick logo
    for p in imgs:
        lo = os.path.basename(p).lower()
        if ("logo" in lo) or ("ait" in lo):
            logo = p; break
    if not logo and imgs: logo = imgs[0]
    # Pick VO
    for p in auds:
        lo = os.path.basename(p).lower()
        if ("vo" in lo) or ("voice" in lo) or ("narr" in lo):
            vo = p; break
    # Pick music
    for p in auds:
        if p == vo: continue
        lo = os.path.basename(p).lower()
        if ("music" in lo) or ("bg" in lo) or ("bed" in lo) or ("track" in lo):
            music = p; break
    if not vo and auds: vo = auds[0]
    if not music and len(auds) >= 2: music = [a for a in auds if a != vo][0]
    return logo, vo, music

def add_color_strip(vse, name, color, channel, f_start, f_end):
    st = vse.sequences.new_effect(name=name, type='COLOR', channel=channel,
                                  frame_start=f_start, frame_end=f_end)
    st.color = color[:3]  # RGB only
    return st

def add_text_strip(vse, name, text, channel, f_start, f_end,
                   size=0.08, x=0.5, y=0.5, color=(1,1,1,1)):
    st = vse.sequences.new_effect(
        name=name, type='TEXT', channel=channel,
        frame_start=f_start, frame_end=f_end
    )
    st.text = text
    # Blender 4.5 uses font_size (unitless, ~0.05–0.12 feels good at 1080p)
    st.font_size = size
    st.color = color
    # Location is normalized [0..1], (0.5, 0.5) is center
    st.location = (x, y)
    # Wrap width (0..1) to keep long lines from touching edges
    st.wrap_width = 0.9
    # Newer builds dropped align_x/align_y; guard them
    if hasattr(st, "align_x"):
        st.align_x = 'CENTER'
    if hasattr(st, "align_y"):
        st.align_y = 'CENTER'
    # Composite mode + opacity (works across 3.x/4.x)
    st.blend_type = 'ALPHA_OVER'
    st.blend_alpha = 1.0
    return st

def add_image_strip(vse, name, filepath, channel, f_start, f_end):
    st = vse.sequences.new_image(name=name, filepath=filepath, channel=channel, frame_start=f_start)
    st.frame_final_start = f_start
    st.frame_final_end   = f_end
    return st

def add_sound_strip(vse, name, filepath, channel, f_start, f_end, volume=1.0):
    st = vse.sequences.new_sound(name=name, filepath=filepath, channel=channel, frame_start=f_start)
    st.frame_final_start = f_start
    st.frame_final_end   = f_end
    st.volume = volume
    return st

def add_transform(vse, name, source_strip, channel):
    st = vse.sequences.new_effect(
        name=name,
        type='TRANSFORM',
        channel=channel,
        frame_start=source_strip.frame_final_start,
        frame_end=source_strip.frame_final_end,
        input1=source_strip  # Blender 4.5: use input1, not seq1
    )
    return st


def key_opacity(st, frame, value):
    st.blend_alpha = value
    st.keyframe_insert(data_path="blend_alpha", frame=frame)

def key_volume(st, frame, vol):
    st.volume = vol
    st.keyframe_insert(data_path="volume", frame=frame)

# -----------------------------
# BUILD MAIN VSE
# -----------------------------
ensure_clean_start()
sc = ensure_scene_main()
vse = sc.sequence_editor

logo_path, vo_path, music_path = find_media()

# Background navy for all 30s
bg = add_color_strip(vse, "BG_Navy", NAVY, channel=1, f_start=1, f_end=F_END)

# Opening text (fade in/out)
t1 = add_text_strip(vse, "Hook", HOOK_TEXT, channel=3,
                    f_start=OPEN_START, f_end=OPEN_END,
                    size=0.105, x=0.5, y=0.58)
t1.wrap_width = 0.8
key_opacity(t1, OPEN_START, 0.0)
key_opacity(t1, OPEN_START + 10, 1.0)
key_opacity(t1, OPEN_END - 20, 1.0)
key_opacity(t1, OPEN_END, 0.0)

# Second line (fade)
t2 = add_text_strip(vse, "Line2", LINE2_TEXT, channel=3,
                    f_start=LINE2_START, f_end=LINE2_END,
                    size=0.085, x=0.5, y=0.50)
t2.wrap_width = 0.8
key_opacity(t2, LINE2_START, 0.0)
key_opacity(t2, LINE2_START + 10, 1.0)
key_opacity(t2, LINE2_END - 10, 1.0)
key_opacity(t2, LINE2_END, 0.0)

# MIDS
midA = add_text_strip(vse, "MidA", MID_A_TEXT, channel=3,
                      f_start=MID_A_START, f_end=MID_A_END,
                      size=0.085, x=0.5, y=0.53); midA.wrap_width = 0.8
key_opacity(midA, MID_A_START, 0.0); key_opacity(midA, MID_A_START + 8, 1.0)
key_opacity(midA, MID_A_END - 8, 1.0); key_opacity(midA, MID_A_END, 0.0)

midB = add_text_strip(vse, "MidB", MID_B_TEXT, channel=3,
                      f_start=MID_B_START, f_end=MID_B_END,
                      size=0.085, x=0.5, y=0.49); midB.wrap_width = 0.8
key_opacity(midB, MID_B_START, 0.0); key_opacity(midB, MID_B_START + 8, 1.0)
key_opacity(midB, MID_B_END - 8, 1.0); key_opacity(midB, MID_B_END, 0.0)

midC = add_text_strip(vse, "MidC", MID_C_TEXT, channel=6,
                      f_start=MID_C_START, f_end=MID_C_END,
                      size=0.095, x=0.5, y=0.56); midC.wrap_width = 0.8
key_opacity(midC, MID_C_START, 0.0); key_opacity(midC, MID_C_START + 8, 1.0)
key_opacity(midC, MID_C_END - 8, 1.0); key_opacity(midC, MID_C_END, 0.0)

# END CARD (frames 750–900)
build_endcard(vse, ENDCARD_START, ENDCARD_END)


# HOOK
t1 = add_text_strip(vse, "Hook", HOOK_TEXT, channel=3, f_start=OPEN_START, f_end=OPEN_END,
                    size=0.105, x=0.5, y=0.58)
t1.wrap_width = 0.8

# LINE2
t2 = add_text_strip(vse, "Line2", LINE2_TEXT, channel=3, f_start=LINE2_START, f_end=LINE2_END,
                    size=0.085, x=0.5, y=0.50)
t2.wrap_width = 0.8

# Logo (if available) with slow zoom transform
if logo_path:
    logo = add_image_strip(vse, "AIT_Logo", logo_path, channel=4, f_start=OPEN_END, f_end=LINE2_END)
    # Simple fades only (no zoom) — fully compatible in 4.5
    # fade in: 150→160
    logo.blend_alpha = 0.0
    logo.keyframe_insert(data_path="blend_alpha", frame=OPEN_END)
    logo.blend_alpha = 1.0
    logo.keyframe_insert(data_path="blend_alpha", frame=OPEN_END + 10)
    # fade out: 290→300
    logo.blend_alpha = 1.0
    logo.keyframe_insert(data_path="blend_alpha", frame=LINE2_END - 10)
    logo.blend_alpha = 0.0
    logo.keyframe_insert(data_path="blend_alpha", frame=LINE2_END)


# Mid text beats
# MIDS
# MIDS
midA = add_text_strip(vse, "MidA", MID_A_TEXT, channel=3, f_start=MID_A_START, f_end=MID_A_END,
                      size=0.085, x=0.5, y=0.53); midA.wrap_width = 0.8
midB = add_text_strip(vse, "MidB", MID_B_TEXT, channel=3, f_start=MID_B_START, f_end=MID_B_END,
                      size=0.085, x=0.5, y=0.49); midB.wrap_width = 0.8
midC = add_text_strip(vse, "MidC", MID_C_TEXT, channel=6, f_start=MID_C_START, f_end=MID_C_END,
                      size=0.095, x=0.5, y=0.56); midC.wrap_width = 0.8

# END CARD (frames 750–900)
# END CARD (frames 750–900)
build_endcard(vse, ENDCARD_START, ENDCARD_END)






# Audio: Voice + Music (ducking)
if os.path.isfile(MP4_OUT):  # avoid trying to read previous render as audio
    try: os.remove(MP4_OUT)
    except: pass

if music_path:
    music = add_sound_strip(vse, "Music", music_path, channel=1, f_start=1, f_end=F_END, volume=0.6)
    key_volume(music, 1, 0.55)
    key_volume(music, ENDCARD_END-60, 0.55)  # keep low under VO till last 2s
    key_volume(music, ENDCARD_END-60, 0.75)  # quick swell
    key_volume(music, ENDCARD_END, 0.75)

if vo_path:
    vo = add_sound_strip(vse, "VoiceOver", vo_path, channel=2, f_start=1, f_end=F_END, volume=1.0)

# -----------------------------
# Build Infinity Scene (3D)
# -----------------------------
def build_infinity_scene():
    sc_inf = bpy.data.scenes.new("Infinity")
    sc_inf.render.resolution_x = 1920
    sc_inf.render.resolution_y = 1080
    sc_inf.render.fps = FPS
    sc_inf.frame_start = INF_START
    sc_inf.frame_end = INF_END
    sc_inf.view_settings.view_transform = 'Filmic'
    sc_inf.view_settings.look = 'None'

    # World
    world = bpy.data.worlds.new("World_Inf")
    world.color = (NAVY[0]*0.6, NAVY[1]*0.6, NAVY[2]*0.6)
    sc_inf.world = world

    col = bpy.data.collections.new("InfinityCol")
    sc_inf.collection.children.link(col)

    # Camera
    cam_data = bpy.data.cameras.new("InfCam")
    cam = bpy.data.objects.new("InfCam", cam_data)
    cam.location = (0.0, -3.0, 0.5)
    cam.rotation_euler = (math.radians(90), 0.0, 0.0)
    col.objects.link(cam)
    sc_inf.camera = cam

    # Light
    light_data = bpy.data.lights.new("Fill", 'AREA')
    light_data.energy = 500
    light = bpy.data.objects.new("Fill", light_data)
    light.location = (0.0, -2.0, 2.0)
    col.objects.link(light)

    # Curve (∞)
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

    # Emission material
    mat = bpy.data.materials.new("InfMat")
    mat.use_nodes = True
    nodes = mat.node_tree.nodes
    for n in list(nodes):
        if n.type != 'OUTPUT_MATERIAL':
            nodes.remove(n)
    emis = nodes.new('ShaderNodeEmission')
    emis.inputs['Color'].default_value = RED
    emis.inputs['Strength'].default_value = 2.5
    out = nodes.get('Material Output')
    mat.node_tree.links.new(emis.outputs['Emission'], out.inputs['Surface'])
    curve_obj.data.materials.append(mat)

    # Animate draw-on via bevel factors
    try:
        curve_data.bevel_factor_start = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_START)
        curve_data.bevel_factor_end = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_START)

        curve_data.bevel_factor_start = 0.0
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_START+120)
        curve_data.bevel_factor_end = 1.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_START+120)

        curve_data.bevel_factor_start = 0.15
        curve_data.keyframe_insert(data_path="bevel_factor_start", frame=INF_END)
        curve_data.bevel_factor_end = 1.0
        curve_data.keyframe_insert(data_path="bevel_factor_end", frame=INF_END)
    except:
        pass

    return sc_inf

sc_inf = build_infinity_scene()

# Insert Infinity scene as a scene strip
inf_strip = vse.sequences.new_scene(name="InfinityScene", scene=sc_inf, channel=8, frame_start=INF_START)
inf_strip.frame_final_start = INF_START
inf_strip.frame_final_end = INF_END

# -----------------------------
# SAVE + (Optional) RENDER
# -----------------------------
# Save the .blend so you can open and tweak in Blender GUI later
bpy.ops.wm.save_as_mainfile(filepath=BLEND_OUT)

# Render the MP4 from CLI run
# Comment this out if you want to render manually later.
bpy.ops.render.render(animation=True, write_still=False)

print("✅ Done. Blend saved:", BLEND_OUT)
print("✅ Rendered MP4:", MP4_OUT)


