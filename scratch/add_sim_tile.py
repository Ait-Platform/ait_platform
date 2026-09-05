import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add the Simulator tile right before Workshop Documents
old_tile = """        <!-- Workshop Documents -->
        <a href="{{ url_for('sace_bp.reading_workshop_docs') }}" class="flex flex-col p-6 border-2 border-teal-500 rounded-xl bg-teal-50 hover:bg-teal-100 transition shadow-sm group">"""

new_tile = """        <!-- THE SIMULATOR (HIGHLY RECOMMENDED) -->
        <a href="{{ url_for('sace_bp.simulator') }}" class="flex flex-col p-6 border-4 border-indigo-500 rounded-xl bg-indigo-50 hover:bg-indigo-100 transition shadow-lg group md:col-span-2 relative overflow-hidden">
            <div class="absolute top-0 right-0 bg-indigo-600 text-white text-xs font-bold px-3 py-1 rounded-bl-lg uppercase tracking-wider shadow-sm">
                Recommended Evaluation Method
            </div>
            <div class="flex items-center mb-4">
                <div class="h-16 w-16 bg-indigo-600 text-white rounded-lg flex items-center justify-center font-bold text-3xl shadow-sm"><i class="fas fa-vr-cardboard"></i></div>
                <div class="ml-4">
                    <h3 class="text-2xl font-bold text-slate-900 mb-1">Test Drive via Simulator</h3>
                    <p class="text-slate-600 text-sm font-medium">Evaluate the interactive ecosystem natively.</p>
                </div>
            </div>
            <p class="text-sm text-slate-600 mb-4 flex-grow">
                Launch the split-screen Evaluation Simulator. This view allows you to act as both the Facilitator and Participant simultaneously, visualizing the exact flow, live synchronization, and interactive polls while dynamically referencing the SACE compliance annexures.
            </p>
            <div class="flex items-center text-indigo-700 font-bold group-hover:underline">
                Launch Simulator Now <i class="fas fa-arrow-right ml-2"></i>
            </div>
        </a>

        <!-- Workshop Documents -->
        <a href="{{ url_for('sace_bp.reading_workshop_docs') }}" class="flex flex-col p-6 border-2 border-teal-500 rounded-xl bg-teal-50 hover:bg-teal-100 transition shadow-sm group">"""

if old_tile in text:
    text = text.replace(old_tile, new_tile)
    with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added simulator tile")
else:
    print("Could not find insertion point")
