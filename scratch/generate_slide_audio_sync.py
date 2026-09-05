import os
import subprocess

# Voice Mappings
VOICE_TITLE = "en-ZA-LeahNeural"
VOICE_CONTENT_RYAN = "en-GB-RyanNeural" 
VOICE_CONTENT_ARIA = "en-US-AriaNeural"
VOICE_FOOTER = "en-ZA-LukeNeural"

out_dir = os.path.abspath(os.path.join("app", "static", "sace_slides", "audio"))
tmp_dir = os.path.join(out_dir, "tmp")

slides = [
    {
        "slide": 1,
        "title": "Introduction to LITRE",
        "ryan": "Welcome to the LITRE Blending Machine methodology. This program is designed to revolutionize early childhood reading.",
        "aria": "",
        "footer": "Slide 1. Please proceed to the next module."
    },
    {
        "slide": 2,
        "title": "The Core Philosophy",
        "ryan": "Reading is not just visual; it is highly interactive.",
        "aria": "By combining tactile feedback with phonics, we engage multiple learning centers in the brain.",
        "footer": "Slide 2. Core principles."
    },
    {
        "slide": 3,
        "title": "The English Family",
        "ryan": "Once upon a time, there was a family of letters. The vowels and the consonants lived together.",
        "aria": "",
        "footer": "Slide 3. Storytelling approach."
    },
    {
        "slide": 4,
        "title": "Meet the Vowels",
        "ryan": "The vowels are the loud, energetic members of the family.",
        "aria": "A, E, I, O, U. They give voice and shape to every word we speak.",
        "footer": "Slide 4. Vowel recognition."
    },
    {
        "slide": 5,
        "title": "Meet the Consonants",
        "ryan": "The consonants are the quiet helpers. They frame the vowels and give them structure.",
        "aria": "",
        "footer": "Slide 5. Consonant recognition."
    },
    {
        "slide": 6,
        "title": "The Palm as the Meeting Place",
        "ryan": "In the LITRE method, the palm of the hand represents our workspace.",
        "aria": "This physical representation allows learners to literally hold the sounds in their hands.",
        "footer": "Slide 6. Tactile learning."
    },
    {
        "slide": 7,
        "title": "Placing the Vowels",
        "ryan": "We assign each vowel to a specific point on the hand. This creates a spatial map for the learner.",
        "aria": "",
        "footer": "Slide 7. Spatial mapping."
    },
    {
        "slide": 8,
        "title": "The Consonant Approach",
        "ryan": "Consonants approach the palm to meet the vowels.",
        "aria": "This movement represents the blending process, turning isolated letters into syllables.",
        "footer": "Slide 8. The blending concept."
    },
    {
        "slide": 9,
        "title": "The Blending Machine in Action",
        "ryan": "Watch as the consonant connects with the vowel.",
        "aria": "B approaches A, creating the sound: BA. The machine is now running.",
        "footer": "Slide 9. Practical demonstration."
    },
    {
        "slide": 10,
        "title": "Syllable Construction",
        "ryan": "From simple two-letter blends, we build the foundation of all words.",
        "aria": "",
        "footer": "Slide 10. Building blocks."
    }
]

for i in range(11, 31):
    slides.append({
        "slide": i,
        "title": f"Module Step {i}",
        "ryan": f"This is step {i} of the LITRE methodology. Here we expand on the blending machine concepts.",
        "aria": "Continuing the interactive sequence for optimal phonemic awareness." if i % 2 == 0 else "",
        "footer": f"Slide {i}. Progression checkpoint."
    })

def gen():
    for s in slides:
        n = s["slide"]
        print(f"Slide {n}...")
        files_to_concat = []
        
        # 1. Title
        title_file = os.path.join(tmp_dir, f"{n}_title.mp3")
        subprocess.run(["edge-tts", "--voice", VOICE_TITLE, "--text", s["title"], "--write-media", title_file])
        files_to_concat.append(title_file)
        
        # 2. Ryan
        if s["ryan"]:
            ryan_file = os.path.join(tmp_dir, f"{n}_ryan.mp3")
            subprocess.run(["edge-tts", "--voice", VOICE_CONTENT_RYAN, "--text", s["ryan"], "--write-media", ryan_file])
            files_to_concat.append(ryan_file)
            
        # 3. Aria
        if s["aria"]:
            aria_file = os.path.join(tmp_dir, f"{n}_aria.mp3")
            subprocess.run(["edge-tts", "--voice", VOICE_CONTENT_ARIA, "--text", s["aria"], "--write-media", aria_file])
            files_to_concat.append(aria_file)
            
        # 4. Footer
        if s["footer"]:
            footer_file = os.path.join(tmp_dir, f"{n}_footer.mp3")
            subprocess.run(["edge-tts", "--voice", VOICE_FOOTER, "--text", s["footer"], "--write-media", footer_file])
            files_to_concat.append(footer_file)
            
        # ffmpeg
        concat_txt = os.path.join(tmp_dir, f"{n}_concat.txt")
        with open(concat_txt, "w", encoding="utf-8") as f:
            for file in files_to_concat:
                f.write(f"file '{os.path.basename(file)}'\n")
                
        final_out = os.path.join(out_dir, f"{n}.mp3")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", os.path.basename(concat_txt), "-c", "copy", final_out]
        # use cwd=tmp_dir and absolute path for final_out
        subprocess.run(cmd, cwd=tmp_dir)
        
gen()
print("Done!")
