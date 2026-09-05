import re

file_path = 'templates/program_sace/post_test/results.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

old_pass_box = r'(<div class="bg-indigo-50 border border-indigo-200 p-6 rounded-xl mb-8">.*?<h3 class="font-bold text-indigo-900 mb-2">.*?</form>\s*</div>)'

new_pass_box = '''<div class="bg-green-50 border border-green-200 p-6 rounded-xl mb-8 shadow-sm">
                <h3 class="font-bold text-green-900 mb-2 text-2xl"><i class="fas fa-trophy text-yellow-500 mr-2"></i> Congratulations, you passed!</h3>
                <p class="text-green-800 mb-6 font-medium">You successfully completed the workshop and achieved a perfect score on the Post-Test. Your certificate of attendance is ready.</p>
                
                <div class="bg-white p-5 rounded-lg border border-green-100">
                    <h4 class="font-bold text-slate-800 mb-2"><i class="fas fa-envelope text-indigo-500 mr-2"></i> Email My Certificate</h4>
                    <p class="text-sm text-slate-600 mb-4">Please confirm or update the email address where you would like it sent.</p>
                    <form action="{{ url_for('sace_bp.email_certificate') }}" method="POST" class="flex items-end gap-4">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                        <div class="flex-grow">
                            <label class="block text-sm font-bold text-slate-700 mb-1">Email Address</label>
                            <input type="email" name="email" value="{{ current_user.email }}" required autofocus class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition">
                        </div>
                        <button type="submit" class="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">
                            <i class="fas fa-paper-plane mr-2"></i> Send Certificate
                        </button>
                    </form>
                </div>
            </div>'''

text = re.sub(old_pass_box, new_pass_box, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
