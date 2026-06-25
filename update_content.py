import os
import re

directory = r"D:\Users\yeshk\Documents\ait_platform\templates\subject_home"

contents = {
    27: """<!-- Theory Section -->
        <div class="bg-white border border-gray-200 rounded-xl p-8 mb-10 shadow-sm">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Key Concepts</h2>
            <div class="space-y-8 text-gray-700">
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Spatial Reasoning</h3>
                    <p class="mb-2">Spatial reasoning is the ability to understand and remember the spatial relations among objects or space.</p>
                </div>
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Navigation</h3>
                    <p class="mb-2">Using spatial awareness to understand where you are, and how to move from one place to another.</p>
                </div>
            </div>
            <div class="mt-10 bg-blue-600 text-white p-6 rounded-xl shadow-md">
                <h3 class="text-xl font-bold mb-4 flex items-center">
                    <svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                    Chapter Summary
                </h3>
                <ul class="space-y-2">
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Spatial reasoning helps us understand how objects fit together.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>It is essential for navigating the physical world.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Spatial awareness allows us to manipulate shapes and objects in our minds.</li>
                </ul>
            </div>
        </div>""",
    28: """<!-- Theory Section -->
        <div class="bg-white border border-gray-200 rounded-xl p-8 mb-10 shadow-sm">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Key Concepts</h2>
            <div class="space-y-8 text-gray-700">
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Logic</h3>
                    <p class="mb-2">Logic is the process of using formal methods to evaluate reasoning and reach sensible conclusions.</p>
                </div>
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Deduction</h3>
                    <p class="mb-2">Using facts, clues, and evidence to figure out what must be true.</p>
                </div>
            </div>
            <div class="mt-10 bg-blue-600 text-white p-6 rounded-xl shadow-md">
                <h3 class="text-xl font-bold mb-4 flex items-center">
                    <svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                    Chapter Summary
                </h3>
                <ul class="space-y-2">
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Logic helps us form step-by-step conclusions.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>It relies on evaluating given facts and evidence.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Logical reasoning prevents us from making assumptions.</li>
                </ul>
            </div>
        </div>""",
    29: """<!-- Theory Section -->
        <div class="bg-white border border-gray-200 rounded-xl p-8 mb-10 shadow-sm">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Key Concepts</h2>
            <div class="space-y-8 text-gray-700">
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Mathematics</h3>
                    <p class="mb-2">The study of numbers, quantities, shapes, and patterns to measure and calculate the world around us.</p>
                </div>
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Calculation</h3>
                    <p class="mb-2">Using mathematical operations to solve everyday problems.</p>
                </div>
            </div>
            <div class="mt-10 bg-blue-600 text-white p-6 rounded-xl shadow-md">
                <h3 class="text-xl font-bold mb-4 flex items-center">
                    <svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                    Chapter Summary
                </h3>
                <ul class="space-y-2">
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Mathematics gives us a universal language for quantities.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Calculation allows us to predict and measure outcomes accurately.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Mathematical patterns exist in almost every aspect of nature and engineering.</li>
                </ul>
            </div>
        </div>""",
    30: """<!-- Theory Section -->
        <div class="bg-white border border-gray-200 rounded-xl p-8 mb-10 shadow-sm">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Key Concepts</h2>
            <div class="space-y-8 text-gray-700">
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Critical Thinking</h3>
                    <p class="mb-2">The objective analysis and evaluation of an issue or information in order to form a judgment.</p>
                </div>
                <div>
                    <h3 class="text-xl font-semibold text-blue-700 mb-2">Evaluation</h3>
                    <p class="mb-2">Weighing different choices, considering perspectives, and making an informed, rational decision.</p>
                </div>
            </div>
            <div class="mt-10 bg-blue-600 text-white p-6 rounded-xl shadow-md">
                <h3 class="text-xl font-bold mb-4 flex items-center">
                    <svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                    Chapter Summary
                </h3>
                <ul class="space-y-2">
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Critical thinking requires us to question assumptions.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>It helps us identify biases and flawed logic in arguments.</li>
                    <li class="flex items-start"><span class="mr-2">&bull;</span>Effective problem solving depends heavily on critical evaluation.</li>
                </ul>
            </div>
        </div>"""
}

for i in range(27, 31):
    filename = f"chapter{i}_theory.html"
    filepath = os.path.join(directory, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    match = re.search(r'(<!-- Theory Section -->.*?)(?=\s*</div>\s*<div class="bg-white rounded-b-2xl)', content, re.DOTALL)
    if match:
        new_content = content[:match.start()] + contents[i] + content[match.end():]
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filename}")
    else:
        print(f"Could not find modal body in {filename}")
