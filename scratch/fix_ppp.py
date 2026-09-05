import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix the slides array mapping
old_slides_array = r"const slides = \[.*?\];"
new_slides_array = "const slides = [\n" + ",\n".join([f"    {{ img: '{{{{ url_for(\\'static\\', filename=\\'sace_slides/{i}.png\\') }}}}' }}" for i in range(1, 31)]) + "\n];"
text = re.sub(old_slides_array, new_slides_array, text, flags=re.DOTALL)

# Add post-test button logic
old_next_btn = r'<button onclick="nextSlide\(\)" id="btn-next".*?</button>'
new_next_btn = '''<button onclick="nextSlide()" id="btn-next" class="w-14 h-14 flex items-center justify-center bg-indigo-600 hover:bg-indigo-700 text-white shadow-lg rounded-full transition disabled:opacity-30 disabled:cursor-not-allowed">
                    <i class="fas fa-chevron-right text-xl"></i>
                </button>
                <a href="{{ url_for('sace_bp.post_test') }}" id="btn-finish" class="hidden px-6 py-3 items-center justify-center bg-green-500 hover:bg-green-400 text-white shadow-[0_0_15px_rgba(34,197,94,0.4)] rounded-xl font-bold transition">
                    <i class="fas fa-flag-checkered mr-2"></i> Take Post-Test
                </a>'''
text = re.sub(old_next_btn, new_next_btn, text, flags=re.DOTALL)

old_btn_update = r"document\.getElementById\('btn-next'\)\.disabled = \(currentIndex === slides\.length - 1\);"
new_btn_update = '''
        if (currentIndex === slides.length - 1) {
            document.getElementById('btn-next').classList.add('hidden');
            document.getElementById('btn-finish').classList.remove('hidden');
            document.getElementById('btn-finish').classList.add('flex');
        } else {
            document.getElementById('btn-next').classList.remove('hidden');
            document.getElementById('btn-finish').classList.add('hidden');
            document.getElementById('btn-finish').classList.remove('flex');
            document.getElementById('btn-next').disabled = false;
        }
'''
text = text.replace(old_btn_update, new_btn_update)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
