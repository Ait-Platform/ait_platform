import re

with open('templates/program_billing/utilities_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

button_old = '''          <div class="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4 border-t border-slate-200">'''

button_new = '''          <div class="grid grid-cols-1 md:grid-cols-3 gap-6 pt-4 border-t border-slate-200">'''

text = text.replace(button_old, button_new)

action_old = '''            </button>

          </div>'''

action_new = '''            </button>

            <!-- Exceptions Action -->
            <button type="submit" name="action" value="exceptions" class="relative block w-full border-2 border-dashed border-slate-300 rounded-xl p-8 text-center hover:border-red-500 hover:bg-red-50 transition duration-150 group">
              <div class="mx-auto flex items-center justify-center h-16 w-16 rounded-full bg-red-100 text-red-600 group-hover:bg-red-200 transition">
                <svg class="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <h3 class="mt-4 text-xl font-bold text-slate-900 group-hover:text-red-800">Meter Exceptions</h3>
              <p class="mt-2 text-sm text-slate-500 group-hover:text-red-600">Review historical records of replaced, stolen, or otherwise inactive meters for this property.</p>
            </button>

          </div>'''

text = text.replace(action_old, action_new)

with open('templates/program_billing/utilities_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Added Exceptions action button to utilities_hub.html')
