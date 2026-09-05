import re

with open('scratch/test_out.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix totalSlides
text = text.replace("const totalSlides = 11;", "const totalSlides = 12;")
text = text.replace("Step <span id=\"f-counter\">0</span> of 11", "Step <span id=\"f-counter\">0</span> of 12")

# Fix P view logic in updateSlides()
old_p_logic = """        let pIndex = 1; // default projector view
        if (currentSlide === 0) pIndex = 0;
        if (currentSlide === 3) pIndex = 2;
        if (currentSlide === 4) pIndex = 3;
        if (currentSlide === 5) pIndex = 4;
        if (currentSlide === 8) pIndex = 5;
        if (currentSlide === 9) pIndex = 8;
        if (currentSlide === 10) pIndex = 6;
        if (currentSlide === 11) pIndex = 9;"""

new_p_logic = """        let pIndex = 1; // default projector view
        if (currentSlide === 1) pIndex = 0;
        if (currentSlide === 4) pIndex = 2;
        if (currentSlide === 5) pIndex = 3;
        if (currentSlide === 6) pIndex = 4;
        if (currentSlide === 9) pIndex = 5;
        if (currentSlide === 10) pIndex = 8;
        if (currentSlide === 11) pIndex = 6;
        if (currentSlide === 12) pIndex = 9;"""

text = text.replace(old_p_logic, new_p_logic)

# Fix Dice Roll logic in triggerRandomDice
old_dice_logic = """    function triggerRandomDice(slideNum) {
        // Slide 2: Pre-test true/false
        if (slideNum === 2) {
            const trues = Math.floor(Math.random() * 15) + 5; // 5 to 20
            const falses = 20 - trues;
            animateBar('slide-2', 0, trues, falses);
        }
        // Slide 4: Root Cause A/B/C/D
        if (slideNum === 4) {
            animateBar('slide-4', 0, Math.floor(Math.random() * 10), Math.floor(Math.random() * 5), Math.floor(Math.random() * 3), 2);
        }
        // Slide 5: Top Challenges
        if (slideNum === 5) {
            animateBar('slide-5', 0, Math.floor(Math.random() * 8), Math.floor(Math.random() * 8), Math.floor(Math.random() * 4));
        }
    }"""

new_dice_logic = """    function triggerRandomDice(slideNum) {
        // Slide 2: Pre-test true/false tally (moved from old 2 to new 2)
        if (slideNum === 2) {
            const trues = Math.floor(Math.random() * 15) + 5; // 5 to 20
            const falses = 20 - trues;
            animateBar('slide-2', 0, trues, falses);
        }
        // Slide 5: Root Cause A/B/C/D (moved from 4 to 5)
        if (slideNum === 5) {
            animateBar('slide-5', 0, Math.floor(Math.random() * 10), Math.floor(Math.random() * 5), Math.floor(Math.random() * 3), 2);
        }
        // Slide 6: Top Challenges (moved from 5 to 6)
        if (slideNum === 6) {
            animateBar('slide-6', 0, Math.floor(Math.random() * 8), Math.floor(Math.random() * 8), Math.floor(Math.random() * 4));
        }
    }"""
text = text.replace(old_dice_logic, new_dice_logic)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
