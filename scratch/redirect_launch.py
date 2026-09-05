import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Change the "Launch Program" button from switchTab('f') to a direct link to the real F Board
old_btn = r"""<button onclick="switchTab\('f'\)" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-xl rounded-xl shadow-\[0_0_15px_rgba\(79,70,229,0\.4\)\] transition flex items-center justify-center mx-auto w-full md:w-auto">
                            <i class="fas fa-play-circle mr-3 text-2xl"></i> Launch Program \(Go to F Board\)
                        </button>"""

new_btn = """<a href="{{ url_for('sace_bp.facilitator_dashboard') }}" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-xl rounded-xl shadow-[0_0_15px_rgba(79,70,229,0.4)] transition flex items-center justify-center mx-auto w-full md:w-auto">
                            <i class="fas fa-play-circle mr-3 text-2xl"></i> Launch Program (Go to F Board)
                        </a>"""

text = re.sub(old_btn, new_btn, text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
