import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

bad_block = """        // ACTIVE STATE
        document.getElementById('controls-lobby').classList.add('hidden');
        document.getElementById('controls-active').classList.remove('hidden');

        
    }"""

good_block = """        // ACTIVE STATE
        document.getElementById('controls-lobby').classList.add('hidden');
        document.getElementById('controls-active').classList.remove('hidden');

        const activeSlide = document.getElementById('slide-' + currentSlide);
        if (activeSlide) {
            activeSlide.classList.remove('hidden');
            activeSlide.classList.add('flex');
        }

        // Update counter
        const counterEl = document.getElementById('slide-counter');
        if (counterEl) {
            counterEl.innerText = currentSlide + " / " + totalSlides;
        }
    }"""

if bad_block in text:
    print("Found the bad block! Replacing...")
    text = text.replace(bad_block, good_block)
else:
    print("Could not find the exact bad block. Searching with regex...")
    # Just insert it before     function prevSlide()
    text = re.sub(r"(// ACTIVE STATE\s*document\.getElementById\('controls-lobby'\)\.classList\.add\('hidden'\);\s*document\.getElementById\('controls-active'\)\.classList\.remove\('hidden'\);\s*)\}", r"\1\n        const activeSlide = document.getElementById('slide-' + currentSlide);\n        if (activeSlide) {\n            activeSlide.classList.remove('hidden');\n            activeSlide.classList.add('flex');\n        }\n\n        const counterEl = document.getElementById('slide-counter');\n        if (counterEl) counterEl.innerText = currentSlide + ' / ' + totalSlides;\n    }", text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
