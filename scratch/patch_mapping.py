import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

old_mapping = '''        // Update P Views (exactly matching the old logic)
        let pIndex = 1; // default projector view
        if (currentSlide === 1) pIndex = 0;
        if (currentSlide === 5) pIndex = 2;
        if (currentSlide === 6) pIndex = 3;
        if (currentSlide === 7) pIndex = 4;
        if (currentSlide === 12) pIndex = 5;
        if (currentSlide === 13) pIndex = 8;
        if (currentSlide === 14) pIndex = 6;
        if (currentSlide === 15) pIndex = 9;'''

new_mapping = '''        // Update P Views (exactly matching the old logic)
        let pIndex = 1; // default projector view
        if (currentSlide === 1) pIndex = 0;
        if (currentSlide === 5) pIndex = 2; // Root Cause
        if (currentSlide === 6) pIndex = 3; // Personal Reflection
        if (currentSlide === 8) pIndex = 4; // Did You Know?
        if (currentSlide === 12) pIndex = 5; // English Family
        if (currentSlide === 13) pIndex = 8; // Game 1
        if (currentSlide === 14) pIndex = 6; // Game 2
        if (currentSlide === 21) pIndex = 9; // Tactile Engagement (Folding)
        if (currentSlide === 22) pIndex = 10; // Final Assessment
        if (currentSlide === 23) pIndex = 7; // Reflection Activity
        if (currentSlide === 24) pIndex = 11; // Results'''

text = text.replace(old_mapping, new_mapping)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
