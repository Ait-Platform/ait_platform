import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add the Launch Program button
new_html = """
                    <div class="mt-8 text-center bg-slate-50 p-8 rounded-xl border border-slate-200 shadow-sm">
                        <h3 class="text-2xl font-bold text-slate-900 mb-2">Ready to evaluate?</h3>
                        <p class="text-slate-600 mb-6">Click the button below to open the Facilitator Program. Once there, press "Start Show" to begin the live synchronization.</p>
                        <button onclick="switchTab('f')" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-xl rounded-xl shadow-[0_0_15px_rgba(79,70,229,0.4)] transition flex items-center justify-center mx-auto w-full md:w-auto">
                            <i class="fas fa-play-circle mr-3 text-2xl"></i> Launch Program (Go to F Board)
                        </button>
                    </div>
                </div>
            </div>
        </div>
"""

# Replace the end of Tab A
text = re.sub(r'                    </div>\s*</div>\s*</div>\s*</div>\s*<!-- Tab F:', new_html + "\n        <!-- Tab F:", text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
