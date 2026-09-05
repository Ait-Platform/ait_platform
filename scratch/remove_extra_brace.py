import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

bad = r"""        const activeSlide = document\.getElementById\('slide-' \+ currentSlide\);
        if \(activeSlide\) \{
            activeSlide\.classList\.remove\('hidden'\);
            activeSlide\.classList\.add\('flex'\);
        \}

        
        \}

        // Update counter"""

good = """        const activeSlide = document.getElementById('slide-' + currentSlide);
        if (activeSlide) {
            activeSlide.classList.remove('hidden');
            activeSlide.classList.add('flex');
        }

        // Update counter"""

text = re.sub(bad, good, text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
