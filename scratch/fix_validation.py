import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Update the "Next" buttons to call a validation function instead of goToStep directly
old_next_1 = r'<button type="button" onclick="goToStep\(2\)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">'
new_next_1 = '<button type="button" onclick="nextStep(1, 2)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">'
text = re.sub(old_next_1, new_next_1, text)

old_next_2 = r'<button type="button" onclick="goToStep\(3\)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">'
new_next_2 = '<button type="button" onclick="nextStep(2, 3)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">'
text = re.sub(old_next_2, new_next_2, text)

# Add validation logic to the script block
old_script = r'<script>\s*function goToStep\(stepNum\) \{'
new_script = '''<script>
    function nextStep(currentStep, nextStepNum) {
        // Validate current step
        const stepContainer = document.getElementById('step-' + currentStep);
        const radioGroups = new Set();
        
        // Find all radio buttons in this step
        stepContainer.querySelectorAll('input[type="radio"]').forEach(radio => {
            radioGroups.add(radio.name);
        });
        
        // Check if every group has a checked radio
        let allValid = true;
        radioGroups.forEach(name => {
            const checked = stepContainer.querySelector(input[name=""]:checked);
            if (!checked) {
                allValid = false;
                // Highlight the missing section
                const groupContainer = stepContainer.querySelector(input[name=""]).closest('.bg-white');
                if (groupContainer) {
                    groupContainer.classList.add('border-red-500', 'ring-2', 'ring-red-200');
                    setTimeout(() => {
                        groupContainer.classList.remove('border-red-500', 'ring-2', 'ring-red-200');
                    }, 2000);
                }
            }
        });
        
        if (!allValid) {
            alert("Please answer all questions before proceeding.");
            return;
        }
        
        goToStep(nextStepNum);
    }

    function goToStep(stepNum) {'''

text = re.sub(old_script, new_script, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
