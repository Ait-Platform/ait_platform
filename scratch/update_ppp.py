import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Update nextSlide to show the Finish button when at the end
old_nextSlide = '''    function nextSlide() {
        if (currentIndex < slides.length - 1) {
            currentIndex++;
            renderSlide();
        }
    }'''

new_nextSlide = '''    function nextSlide() {
        if (currentIndex < slides.length - 1) {
            currentIndex++;
            renderSlide();
            
            // If we just reached the last slide, show the finish button
            if (currentIndex === slides.length - 1) {
                document.getElementById('btn-next').classList.add('hidden');
                document.getElementById('btn-finish').classList.remove('hidden');
            }
        }
    }'''

text = text.replace(old_nextSlide, new_nextSlide)

# Also update the btn-finish href to the new complete route, and change its text
old_btn = '''<a href="{{ url_for('sace_bp.post_test') }}" id="btn-finish" class="hidden px-6 py-3 items-center justify-center bg-green-500 hover:bg-green-400 text-white shadow-[0_0_15px_rgba(34,197,94,0.4)] rounded-xl font-bold transition">
                    <i class="fas fa-flag-checkered mr-2"></i> Take Post-Test
                </a>'''
new_btn = '''<a href="{{ url_for('sace_bp.presentation_complete') }}" id="btn-finish" class="hidden px-6 py-3 items-center justify-center bg-green-500 hover:bg-green-400 text-white shadow-[0_0_15px_rgba(34,197,94,0.4)] rounded-xl font-bold transition flex">
                    <i class="fas fa-flag-checkered mr-2"></i> Finish & Return
                </a>'''

text = text.replace(old_btn, new_btn)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
