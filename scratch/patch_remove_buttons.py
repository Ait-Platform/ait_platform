import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove Prev and Next Slide buttons from the footer
footer_search = '''<button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="prevSlide()">
                                  <i class="fas fa-chevron-left mr-2"></i> Prev Slide
                              </button>
                              <button class="px-6 py-4 bg-slate-300 hover:bg-slate-400 text-slate-700 font-bold rounded-xl transition" onclick="nextSlide()">
                                  Next Slide <i class="fas fa-chevron-right ml-2"></i>
                              </button>
                              '''

text = text.replace(footer_search, '')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
