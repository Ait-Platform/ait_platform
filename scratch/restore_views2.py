import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

assessment_html = """
                    <!-- App View 10: Final Assessment -->
                    <div id="app-view-10" class="app-view hidden overflow-y-auto pb-20">
                        <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
                            <h3 class="text-xl font-bold mb-1"><i class="fas fa-award mr-2"></i>Final Assessment</h3>
                            <p class="text-xs text-indigo-200">Knowledge, Practical Demonstration, & Application</p>
                        </div>
                        
                        <form id="assessment-form" onsubmit="event.preventDefault(); submitAssessment();">
                            
                            <!-- Section A: Knowledge -->
                            <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
                                <h4 class="font-bold text-slate-800 mb-4 border-b pb-2">Section A: Knowledge (MCQ)</h4>
                                
                                <div class="mb-4">
                                    <p class="font-semibold text-sm mb-2 text-slate-700">1. What is the purpose of the LITRE blending-machine concept?</p>
                                    <div class="space-y-1 text-sm">
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q1" value="0" class="mr-2" required> A. To replace reading practice</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q1" value="1" class="mr-2"> B. To provide a physical and visual representation of the blending process</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q1" value="0" class="mr-2"> C. To test handwriting</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q1" value="0" class="mr-2"> D. To teach mathematical concepts</label>
                                    </div>
                                </div>

                                <div class="mb-4">
                                    <p class="font-semibold text-sm mb-2 text-slate-700">2. What does the palm represent in the LITRE model?</p>
                                    <div class="space-y-1 text-sm">
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q2" value="0" class="mr-2" required> A. The classroom</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q2" value="1" class="mr-2"> B. The meeting place in the blending model</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q2" value="0" class="mr-2"> C. The alphabet</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q2" value="0" class="mr-2"> D. The worksheet</label>
                                    </div>
                                </div>
                                
                                <div class="mb-4">
                                    <p class="font-semibold text-sm mb-2 text-slate-700">3. What is the teacher's role during guided practice?</p>
                                    <div class="space-y-1 text-sm">
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q3" value="0" class="mr-2" required> A. Provide all answers</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q3" value="0" class="mr-2"> B. Allow learners to work without guidance</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q3" value="1" class="mr-2"> C. Demonstrate, guide, observe and provide corrective support</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q3" value="0" class="mr-2"> D. Only mark worksheets</label>
                                    </div>
                                </div>
                                
                                <div class="mb-2">
                                    <p class="font-semibold text-sm mb-2 text-slate-700">4. Why is repetition important in catch-up reading?</p>
                                    <div class="space-y-1 text-sm">
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q4" value="1" class="mr-2" required> A. It provides additional opportunities for learners to practise and consolidate skills.</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q4" value="0" class="mr-2"> B. It eliminates the need for teaching.</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q4" value="0" class="mr-2"> C. It prevents learner participation.</label>
                                        <label class="flex items-center p-2 bg-slate-50 rounded border border-slate-100"><input type="radio" name="q4" value="0" class="mr-2"> D. It replaces assessment.</label>
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Section B: Practical Demonstration -->
                            <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-4">
                                <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Section B: Practical Rubric</h4>
                                <p class="text-xs text-slate-500 mb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>
                                
                                <div class="space-y-2 text-sm">
                                    <div class="flex justify-between items-center"><span class="w-2/3">Explains LITRE purpose</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Demonstrates palm model</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Introduces vowels</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Introduces consonants</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Demonstrates blending</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Provides learner guidance</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Uses questioning</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Provides corrective support</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                    <div class="flex justify-between items-center"><span class="w-2/3">Applies methodology to context</span> <select class="w-1/3 p-1 border rounded rubric-score" required><option value="">--</option><option value="0">0</option><option value="1">1</option><option value="2">2</option><option value="3">3</option></select></div>
                                </div>
                            </div>
                            
                            <!-- Section C: Classroom Application -->
                            <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
                                <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Section C: Classroom Application</h4>
                                <p class="text-xs text-slate-500 mb-4">Check all that are successfully included in the lesson activity.</p>
                                <div class="grid grid-cols-2 gap-2 text-xs font-medium text-slate-700">
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Appropriate objective</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Correct sequence</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Practical demo</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Learner participation</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Teacher guidance</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Reading practice</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Assessment</span></label>
                                    <label class="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-100"><input type="checkbox" class="application-check" value="1"><span>Reflection</span></label>
                                </div>
                            </div>
                            
                            <button type="submit" class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition">Submit Final Assessment</button>
                        </form>
                    </div>

                    <!-- App View 11: Results / Certificate Status -->
                    <div id="app-view-11" class="app-view hidden overflow-y-auto">
                        <div id="assessment-result-container" class="bg-white p-6 rounded-xl shadow-lg border border-slate-200 text-center mt-10">
                            <i class="fas fa-spinner fa-spin text-4xl text-indigo-500 mb-4"></i>
                            <p class="text-slate-600">Awaiting assessment results...</p>
                        </div>
                    </div>
"""

# Insert inside the container
content = content.replace("</div>\n\n<script>", assessment_html + "\n</div>\n\n<script>")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
