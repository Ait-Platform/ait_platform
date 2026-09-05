import re

with open('templates/program_sace/onboarding.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_link = """
            <div class="mt-8 text-center border-t border-slate-100 pt-6">
                <p class="text-sm text-slate-500 mb-2">Are you the Facilitator or a SACE Evaluator?</p>
                <a href="{{ url_for('sace_bp.reading_hub') }}" class="text-indigo-600 hover:text-indigo-800 font-medium hover:underline">
                    Skip this step and go to the SACE Hub &rarr;
                </a>
            </div>"""

content = content.replace(old_link, "")

with open('templates/program_sace/onboarding.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed manual bypass link")
