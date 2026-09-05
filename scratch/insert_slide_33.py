import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Update Step 2's Next button to go to Step 3 (which it already does, but we are changing what step 3 is)
# It currently is: <button type="button" onclick="nextStep(2, 3)" ...

# 2. Extract current Step 3 (MCQ) and change its ID to step-4
step3_mcq = re.search(r'(<div id="step-3" class="step-container hidden">.*?</div>\s*</form>)', text, re.DOTALL).group(1)
step4_mcq = step3_mcq.replace('id="step-3"', 'id="step-4"')
# Make sure Step 4 has the submit button, which it already does.

# 3. Create the new Step 3 (Survey)
step3_survey = '''
            <!-- STEP 3: Post-Workshop Survey (Slide 33) -->
            <div id="step-3" class="step-container hidden">
                <div class="bg-purple-50 border border-purple-200 p-8 rounded-xl mb-6 shadow-sm">
                    <h3 class="font-black text-purple-900 mb-4 text-2xl"><i class="fas fa-microscope mr-2"></i> Post-Workshop Survey (Longitudinal Study)</h3>
                    
                    <div class="bg-white p-6 rounded-lg border border-purple-100 text-purple-800 leading-relaxed italic mb-6">
                        "If you watch Snow White today, or 50 years from now, the quality must remain exactly the same." — Inspired by Walt Disney
                        <br><br>
                        <strong>Why we need your interaction:</strong> We are striving for that same timeless standard of quality with the LITRE method. This brief survey is not an assessment of you, but rather a vital data collection point for our longitudinal path analysis. We are tracking whether LITRE truly holds its value and efficacy over time within the schooling system. Your honest feedback here provides our research baseline.
                    </div>
                </div>

                <div class="space-y-6">
                    <!-- Q1 -->
                    <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                        <h4 class="text-lg font-bold text-slate-800 mb-2">1. Teacher Self-Efficacy</h4>
                        <p class="text-sm text-slate-500 mb-4">How confident do you feel in your ability to successfully implement the LITRE blending machine in your classroom?</p>
                        <div class="flex flex-col sm:flex-row space-y-2 sm:space-y-0 sm:space-x-4">
                            <label class="flex flex-col items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition flex-1"><input type="radio" name="efficacy" value="1" required class="mb-2 h-5 w-5 text-purple-600"><span class="text-slate-700 font-bold">1 (Low)</span></label>
                            <label class="flex flex-col items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition flex-1"><input type="radio" name="efficacy" value="2" class="mb-2 h-5 w-5 text-purple-600"><span class="text-slate-700 font-bold">2</span></label>
                            <label class="flex flex-col items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition flex-1"><input type="radio" name="efficacy" value="3" class="mb-2 h-5 w-5 text-purple-600"><span class="text-slate-700 font-bold">3</span></label>
                            <label class="flex flex-col items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition flex-1"><input type="radio" name="efficacy" value="4" class="mb-2 h-5 w-5 text-purple-600"><span class="text-slate-700 font-bold">4</span></label>
                            <label class="flex flex-col items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition flex-1"><input type="radio" name="efficacy" value="5" class="mb-2 h-5 w-5 text-purple-600"><span class="text-slate-700 font-bold">5 (High)</span></label>
                        </div>
                    </div>

                    <!-- Q2 -->
                    <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                        <h4 class="text-lg font-bold text-slate-800 mb-2">2. Perceived Utility</h4>
                        <p class="text-sm text-slate-500 mb-4">Do you believe the physical and visual representation of the LITRE method provides a significant advantage over traditional phonics instruction?</p>
                        <div class="space-y-3">
                            <label class="flex items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition"><input type="radio" name="utility" value="yes" required class="h-5 w-5 text-purple-600"><span class="ml-3 text-slate-700">Yes, definitively</span></label>
                            <label class="flex items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition"><input type="radio" name="utility" value="somewhat" class="h-5 w-5 text-purple-600"><span class="ml-3 text-slate-700">Somewhat</span></label>
                            <label class="flex items-center p-3 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-purple-50 transition"><input type="radio" name="utility" value="no" class="h-5 w-5 text-purple-600"><span class="ml-3 text-slate-700">No significant advantage</span></label>
                        </div>
                    </div>
                </div>

                <div class="pt-8 text-right border-t border-slate-100 mt-8">
                    <button type="button" onclick="nextStep(3, 4)" class="px-8 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">
                        Next: Final Assessment (Slide 34) <i class="fas fa-arrow-right ml-2"></i>
                    </button>
                </div>
            </div>
'''

# 4. Replace the old Step 3 with Step 3 (Survey) + Step 4 (MCQ)
text = text.replace(step3_mcq, step3_survey + "\n" + step4_mcq)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
