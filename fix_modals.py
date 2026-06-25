import os
import re

directory = r"D:\Users\yeshk\Documents\ait_platform\templates\subject_home"

for i in range(21, 31):
    filename = f"chapter{i}_theory.html"
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        print(f"Not found: {filename}")
        continue
        
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    if "<!-- Theory Section -->" not in content or "<!-- Quiz Section -->" not in content:
        print(f"Missing tags: {filename}")
        continue
        
    # Check if already processed
    if "theory-modal" in content:
        print(f"Already processed: {filename}")
        continue
        
    # Extract the theory section exactly until the Quiz section comment
    theory_match = re.search(r'(<!-- Theory Section -->.*?)(?=<!-- Quiz Section -->)', content, re.DOTALL)
    if not theory_match:
        print(f"Regex failed: {filename}")
        continue
        
    theory_content = theory_match.group(1)
    
    # Remove theory_content from the main flow
    new_content = content.replace(theory_content, "")
    
    # Insert Help button right before Quiz Section
    help_btn = """<!-- Help Button -->
        <div class="mb-8 flex justify-end">
            <button type="button" onclick="document.getElementById('theory-modal').classList.remove('hidden')" class="inline-flex items-center px-4 py-2 bg-indigo-100 text-indigo-700 font-bold rounded-lg shadow-sm hover:bg-indigo-200 transition-colors">
                <i class="fa-solid fa-circle-question mr-2"></i> Need Help? (Review Theory)
            </button>
        </div>
        
        """
    new_content = new_content.replace("<!-- Quiz Section -->", help_btn + "<!-- Quiz Section -->")
    
    # Wrap theory_content in a modal and put it before {% endblock %}
    modal = f"""
<!-- Theory Modal -->
<div id="theory-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden bg-black bg-opacity-60 px-4 backdrop-blur-sm transition-opacity duration-300">
    <div class="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col relative transform transition-all">
        <div class="bg-indigo-600 rounded-t-2xl px-6 py-4 flex justify-between items-center z-10 shadow-md">
            <h2 class="text-2xl font-bold text-white flex items-center"><i class="fa-solid fa-book-open mr-3"></i> Theory Review</h2>
            <button type="button" onclick="document.getElementById('theory-modal').classList.add('hidden')" class="text-white hover:text-indigo-200 text-3xl font-bold leading-none focus:outline-none transition-colors">&times;</button>
        </div>
        <div class="p-8 overflow-y-auto bg-gray-50 flex-1">
{theory_content}
        </div>
        <div class="bg-white rounded-b-2xl border-t border-gray-200 px-6 py-4 flex justify-end shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.05)]">
            <button type="button" onclick="document.getElementById('theory-modal').classList.add('hidden')" class="px-8 py-3 bg-indigo-600 text-white font-bold rounded-xl hover:bg-indigo-700 shadow-md transition-colors">Close</button>
        </div>
    </div>
</div>
"""
    # Find the last {% endblock %} and insert right before it
    last_endblock_idx = new_content.rfind("{% endblock %}")
    if last_endblock_idx != -1:
        new_content = new_content[:last_endblock_idx] + modal + "\n" + new_content[last_endblock_idx:]
    else:
        new_content += modal
        
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
        
    print(f"Updated {filename}")
