import re

file_path = 'templates/program_sace/post_test/results.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# I will find the end of the new_pass_box and insert the else block and endif
old_str = '''            </div>

            <!-- Review Section -->'''

new_str = '''            </div>
            {% else %}
            <div class="bg-yellow-50 border border-yellow-200 p-6 rounded-xl mb-8 text-center shadow-sm">
                <h3 class="font-bold text-yellow-900 mb-2 text-xl"><i class="fas fa-exclamation-triangle text-yellow-600 mr-2"></i> Retake Required</h3>
                <p class="text-sm text-yellow-800 mb-6">You did not achieve a perfect score. Please review your answers below, learn from any mistakes, and retake the post-test.</p>
                <a href="{{ url_for('sace_bp.post_test') }}" class="inline-flex items-center justify-center px-8 py-3 text-lg font-bold text-indigo-900 bg-yellow-400 rounded-xl hover:bg-yellow-300 hover:scale-105 transition shadow-lg border border-yellow-500">
                    <i class="fas fa-redo-alt mr-3"></i> Retake Post-Test
                </a>
            </div>
            {% endif %}

            <!-- Review Section -->'''

text = text.replace(old_str, new_str)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
