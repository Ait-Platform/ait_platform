import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Replace slides array to include audio
for i in range(1, 31):
    old_slide = f'{{ img: "{{{{ url_for(\'static\', filename=\'sace_slides/{i}.png\') }}}}" }}'
    new_slide = f'{{ img: "{{{{ url_for(\'static\', filename=\'sace_slides/{i}.png\') }}}}", audio: "{{{{ url_for(\'static\', filename=\'sace_slides/audio/{i}.mp3\') }}}}" }}'
    text = text.replace(old_slide, new_slide)

# Ensure the JS plays the audio
old_js = '''        // Update Image
        document.getElementById('slide-image').src = slide.img;
    }'''

new_js = '''        // Update Image
        document.getElementById('slide-image').src = slide.img;
        
        // Play Audio
        if (slide.audio) {
            currentAudio = new Audio(slide.audio);
            // Optional: Show a little speaker icon or handle autoplay restrictions
            currentAudio.play().catch(e => {
                console.log("Audio play prevented:", e);
                // We could show a 'Play Audio' button if browser blocks autoplay
            });
        }
    }'''
text = text.replace(old_js, new_js)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
